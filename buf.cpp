#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}


BufMgr::~BufMgr() {

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete [] bufTable;
    delete [] bufPool;
}


 /**
  * Allocates a free buffer frame from the current buffer
  * using the clock replacement algorithm
  * 
  * @param frame The frameNo returned if completed successfully
  * 
  * @return BUFFEREXCEEDED if all frames are pinned,
  *     UNIXERR if I/O error when flushing a page,
  *     OK otherwise
  */
const Status BufMgr::allocBuf(int & frame) {
    Status s = OK;
    bool completed = false;
    int bufCount = 0;

    while (!completed) {
        advanceClock();
        bufCount++;

        if (bufCount > (int)(2 * numBufs)) {
            return BUFFEREXCEEDED;
        }

        // bufDesc for the next buffer frame
        BufDesc* desc = &bufTable[clockHand];

        // Check each relevant field

        if (!desc->valid) {
            // unused frame
            frame = desc->frameNo;
            return s;
        }
        if (desc->refbit) {
            desc->refbit = false;
            continue;
        }
        if (desc->pinCnt > 0) {
            continue;
        }
        if (desc->dirty) {
            // flush page to disk
            s = desc->file->writePage(desc->pageNo, &(bufPool[clockHand]));
            if (s != OK) return s;
        }

        // set the frameNo and remove current frame from hashTable
         frame = desc->frameNo;
        s = hashTable->remove(desc->file, desc->pageNo);
        desc->Clear();
        completed = true;
    }
    
    return s;
}

 /**
  * Attempts to read a page from the buffer pool. If the page is in the pool
  * it is returned immediately. If not, a free buffer frame is allocated
  * and the page is read from disk into the buffer pool.
  * 
  * @param file file to read from
  * @param PageNo page number to read
  * @param page reference to the page to read into
  * 
  * @return BUFFEREXCEEDED if all frames are pinned,
  *     UNIXERR if Unix error occurs,
  *     HASHTBLERROR if a hash table error occurs,
  *     OK otherwise
  */
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{
    int frameNo;
    Status s = OK;
    s = hashTable->lookup(file, PageNo, frameNo);
    if (s != OK) {
        // page not in buffer pool, need to read from disk
        s = allocBuf(frameNo);
        if (s != OK) return s;

        s = file->readPage(PageNo, &(bufPool[frameNo]));
        if (s != OK) return s;

        s = hashTable->insert(file, PageNo, frameNo);
        if (s != OK) return s;
        bufTable[frameNo].Set(file, PageNo);
        page = &(bufPool[frameNo]);
    } else {
        // page already in buffer pool
        bufTable[frameNo].refbit = true;
        bufTable[frameNo].pinCnt++;
        page = &(bufPool[frameNo]);
    }


    return OK;
}

/**
  * Decrements the pin count of a page or does nothing
  * if the page is not yet pinned
  * 
  * @param file file to unpin
  * @param PageNo page number to unpin
  * @param dirty indicates whether the page is dirty
  * 
  * @return PAGENOTPINNED if the page is not pinned,
  *     OK otherwise
  */
const Status BufMgr::unPinPage(File* file, const int PageNo, 
			       const bool dirty) 
{
    int frameNo;
    Status s = OK;
    s = hashTable->lookup(file, PageNo, frameNo);
    if (s != OK) return s;

    if (bufTable[frameNo].pinCnt == 0) return PAGENOTPINNED;
    bufTable[frameNo].pinCnt--;
    
    if (dirty) {
        bufTable[frameNo].dirty = true;
    }

    return OK;
}

/**
 * Allocates a new page from a given file and adds it to the buffer
 * 
 * @param file The file to allocate a new page to
 * @param pageNo The returned pageNo of the allocated page
 * @param page The returned page pointer from the pool
 * 
 * @return UNIXERR for I/O errors, BUFFEREXCEEDED if all buffer frames are pinned,
 *      and HASHTBLERROR if a hash table error occurred
 */
const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) {
    // Allocate new page to file, create new buffer frame
    // & insert the frame into the hashTable

    Status s = file->allocatePage(pageNo);
    if (s != OK) return s;
    int frameNo;
    s = allocBuf(frameNo);
    if (s != OK) return s;
    s = hashTable->insert(file, pageNo, frameNo);
    if (s != OK) return s;
    
    // Set the description for the allocated frame

    bufTable[frameNo].Set(file, pageNo);

    // Get the relevant page from the pool

    page = &bufPool[frameNo];
    return OK;
}

const Status BufMgr::disposePage(File* file, const int pageNo) 
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file) 
{
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {

      if (tmpbuf->pinCnt > 0)
	  return PAGEPINNED;

      if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
	cout << "flushing page " << tmpbuf->pageNo
             << " from frame " << i << endl;
#endif
	if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
					      &(bufPool[i]))) != OK)
	  return status;

	tmpbuf->dirty = false;
      }

      hashTable->remove(file,tmpbuf->pageNo);

      tmpbuf->file = NULL;
      tmpbuf->pageNo = -1;
      tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
      return BADBUFFER;
  }
  
  return OK;
}


void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}


