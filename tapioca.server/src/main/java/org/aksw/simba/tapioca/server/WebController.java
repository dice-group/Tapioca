/**
 * The MIT License
 * Copyright (c) 2015 Michael Röder (roeder@informatik.uni-leipzig.de)
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
package org.aksw.simba.tapioca.server;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
public class WebController {

    @Autowired
    private TMEngine engine;

    @Autowired
    private BLEngine blEngine;

    public WebController() {
        System.out.println("Controller created.");
    }

    @RequestMapping("/search")
    public @ResponseBody
    String search(@RequestParam(value = "voidString") String voidString) {
        return engine.retrieveSimilarDatasets(voidString);
        // SearchResult results[] = new SearchResult[result.size()];
        // for (int i = 0; i < results.length; ++i) {
        // results[i] = new SearchResult((Dataset) result.objects[i],
        // result.values[i]);
        // }
        // return new ModelMap(results);
    }

    @RequestMapping("/blsearch")
    public @ResponseBody
    String searchBL(@RequestParam(value = "voidString") String voidString) {
        return blEngine.retrieveSimilarDatasets(voidString);
        // TopDoubleObjectCollection<Dataset> result =
        // blEngine.retrieveSimilarDatasets(voidString);
        // SearchResult results[] = new SearchResult[result.size()];
        // for (int i = 0; i < results.length; ++i) {
        // results[i] = new SearchResult((Dataset) result.objects[i],
        // result.values[i]);
        // }
        // return new ModelMap(results);
    }

    // @RequestMapping({ "/", "/index" })
    // public ModelAndView index() {
    // System.out.println("index called");
    // return new ModelAndView("index");
    // }
}
