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

import java.io.File;

import org.aksw.simba.tapioca.preprocessing.labelretrieving.WorkerBasedLabelRetrievingDocumentSupplierDecorator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

/**
 * 
 * @author Michael R&ouml;der (roeder@informatik.uni-leipzig.de)
 * 
 */
@Configuration
@EnableWebMvc
@ComponentScan(basePackages = "org.aksw.simba.tapioca.server")
@PropertySource("classpath:tapioca.properties")
public class Config extends WebMvcConfigurerAdapter {

    private static final String LABEL_CACHING_FILES_PROPERTY_KEY = "org.aksw.simba.tapioca.server.Config.CacheFiles";
    private static final String MODEL_FOLDER_PROPERTY_KEY = "org.aksw.simba.tapioca.server.Engine.ModelFolder";
    private static final String META_DATA_FILE_PROEPRTY_KEY = "org.aksw.simba.tapioca.server.Engine.MetaDataFile";

    public static @Bean
    WorkerBasedLabelRetrievingDocumentSupplierDecorator createCachingLabelRetriever(Environment env) {
        File labelFiles[] = {}; // TODO add label files
        String cacheFileNames[] = env.getProperty(LABEL_CACHING_FILES_PROPERTY_KEY, (new String[0]).getClass());
        File chacheFiles[] = new File[cacheFileNames.length];
        for (int i = 0; i < chacheFiles.length; ++i) {
            chacheFiles[i] = new File(cacheFileNames[i]);
        }
        return new WorkerBasedLabelRetrievingDocumentSupplierDecorator(null, chacheFiles, labelFiles);
    }

    public static @Bean
    TMEngine createEngine(Environment env, WorkerBasedLabelRetrievingDocumentSupplierDecorator cachingLabelRetriever) {
        return TMEngine.createEngine(cachingLabelRetriever, new File(env.getProperty(MODEL_FOLDER_PROPERTY_KEY)),
                new File(env.getProperty(META_DATA_FILE_PROEPRTY_KEY)));
    }

    public static @Bean
    BLEngine createBLEngine(Environment env) {
        return BLEngine.createEngine(new File(env.getProperty(MODEL_FOLDER_PROPERTY_KEY)));
    }

    @Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {
        registry.addResourceHandler("/webjars/**").addResourceLocations("classpath:/META-INF/resources/webjars/");
    }

    // @Bean
    // public ViewResolver viewResolver() {
    // System.out.println("Creating view resolver...");
    // InternalResourceViewResolver viewResolver = new
    // InternalResourceViewResolver();
    // viewResolver.setPrefix("/WEB-INF/views/");
    // viewResolver.setSuffix(".jsp");
    // return viewResolver;
    // }
}
