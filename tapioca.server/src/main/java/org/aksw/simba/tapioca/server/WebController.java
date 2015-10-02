/**
 * This file is part of tapioca.server.
 *
 * tapioca.server is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * tapioca.server is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with tapioca.server.  If not, see <http://www.gnu.org/licenses/>.
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
