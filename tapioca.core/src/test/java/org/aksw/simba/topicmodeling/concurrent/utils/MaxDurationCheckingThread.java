/**
 * Copyright (C) 2014 Michael Röder (michael.roeder@unister.de)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.aksw.simba.topicmodeling.concurrent.utils;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MaxDurationCheckingThread implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(MaxDurationCheckingThread.class);

    private long duration;
    private boolean finished;
    private Thread watchedThread;

    public MaxDurationCheckingThread(Thread watchedThread, long duration) {
        this.duration = duration;
        this.watchedThread = watchedThread;
    }

    @SuppressWarnings("deprecation")
    @Override
    public void run() {
        finished = false;
        synchronized (this) {
            try {
                wait(duration);
            } catch (InterruptedException e) {
                LOGGER.warn("Interrupted while waiting.");
            }
        }
        try {
            Assert.assertTrue("The thread didn't finished after " + duration + " ms.", finished);
        } catch (AssertionError e) {
            // At this point, we are allowed to use this deprecated method, since this test seemed to fail and we want
            // to stop the whole system. Thus, the reason that led to the deprecation of this method is no problem for
            // us.
            watchedThread.stop(e);
            throw e;
        }
    }

    public void reportFinished() {
        finished = true;
        synchronized (this) {
            this.notify();
        }
    }

}
