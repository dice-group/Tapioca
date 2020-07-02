/**
 * tapioca.core - ${project.description}
 * Copyright © 2015 Data Science Group (DICE) (michael.roeder@uni-paderborn.de)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
/**
 * This file is part of tapioca.core.
 *
 * tapioca.core is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * tapioca.core is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with tapioca.core.  If not, see <http://www.gnu.org/licenses/>.
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
            watchedThread.stop();
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
