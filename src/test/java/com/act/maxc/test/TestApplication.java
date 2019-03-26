///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing,
// * software distributed under the License is distributed on an
// * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// * KIND, either express or implied.  See the License for the
// * specific language governing permissions and limitations
// * under the License.
// */
//
//package com.act.maxc.test;
//
//
//import java.io.File;
//import java.util.List;
//import java.util.Random;
//
//import org.apache.commons.io.FileUtils;
//import org.apache.flume.lifecycle.LifecycleAware;
//import org.apache.flume.node.Application;
//import org.apache.flume.node.PollingPropertiesFileConfigurationProvider;
//import org.junit.After;
//import org.junit.Before;
//import org.junit.Ignore;
//import org.junit.Test;
//
//import com.google.common.collect.Lists;
//import com.google.common.eventbus.EventBus;
//import com.google.common.io.Files;
//
//public class TestApplication {
//
//  private File baseDir;
//
//  @Before
//  public void setup() throws Exception {
//    baseDir = Files.createTempDir();
//  }
//  @After
//  public void tearDown() throws Exception {
//    FileUtils.deleteDirectory(baseDir);
//  }
//
//  @Test
//  public void testFLUME1854() throws Exception {
//    File configFile = new File(baseDir, "flume-conf.properties");
//    Files.copy(new File(getClass().getClassLoader()
//        .getResource("flume-booker-conf.properties").getFile()), configFile);
//    Random random = new Random();
//    
//    for (int i = 0; i < 1; i++) {
//      EventBus eventBus = new EventBus("test-event-bus");
//      PollingPropertiesFileConfigurationProvider configurationProvider =
//          new PollingPropertiesFileConfigurationProvider("host1", configFile, eventBus, 1);
//      List<LifecycleAware> components = Lists.newArrayList();
//      components.add(configurationProvider);
//      Application application = new Application(components);
//      eventBus.register(application);
//      application.start();
//      Thread.sleep(random.nextInt(1000 * 10 * 3600));
//      application.stop();
//    }
//  }
//  
//}
