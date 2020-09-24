/**
 * tapioca.server - ${project.description}
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

import java.io.File;

import org.aksw.simba.tapioca.preprocessing.StringCountToSimpleTokenizedTextConvertingDocumentSupplierDecorator.WordOccurence;
import org.aksw.simba.tapioca.preprocessing.UriCountMappingCreatingDocumentSupplierDecorator.UriUsage;
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

    public static @Bean WorkerBasedLabelRetrievingDocumentSupplierDecorator createCachingLabelRetriever(
            Environment env) {
        File labelFiles[] = {}; // TODO add label files
        String cacheFileNames[] = env.getProperty(LABEL_CACHING_FILES_PROPERTY_KEY, (new String[0]).getClass());
        File chacheFiles[] = new File[cacheFileNames.length];
        for (int i = 0; i < chacheFiles.length; ++i) {
            chacheFiles[i] = new File(cacheFileNames[i]);
        }
        return new WorkerBasedLabelRetrievingDocumentSupplierDecorator(null, chacheFiles, labelFiles);
    }

    public static @Bean TMEngine createEngine(Environment env,
            WorkerBasedLabelRetrievingDocumentSupplierDecorator cachingLabelRetriever) {
        return TMEngine.createEngine(cachingLabelRetriever, new File(env.getProperty(MODEL_FOLDER_PROPERTY_KEY)),
                new File(env.getProperty(META_DATA_FILE_PROEPRTY_KEY)), UriUsage.CLASSES_AND_PROPERTIES, WordOccurence.LOG);
    }

    public static @Bean BLEngine createBLEngine(Environment env) {
        return BLEngine.createEngine(new File(env.getProperty(MODEL_FOLDER_PROPERTY_KEY)),
                new File(env.getProperty(META_DATA_FILE_PROEPRTY_KEY)), UriUsage.CLASSES_AND_PROPERTIES);
    }

    @Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {
        registry.addResourceHandler("/webjars/**").addResourceLocations("classpath:/META-INF/resources/webjars/");
        registry.addResourceHandler("/webResources/**").addResourceLocations("classpath:webResources/");

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