/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import net.sf.uadetector.service.UADetectorServiceFactory;
import net.sf.uadetector.ReadableUserAgent;
import net.sf.uadetector.UserAgent;
import net.sf.uadetector.UserAgentType;
import net.sf.uadetector.UserAgentStringParser;
import net.sf.uadetector.ReadableDeviceCategory;
import net.sf.uadetector.UserAgentFamily;
import net.sf.uadetector.VersionNumber;
import net.sf.uadetector.OperatingSystem;
import net.sf.uadetector.OperatingSystemFamily;

@RunWith(PowerMockRunner.class)
@PrepareForTest({DeviceEnrichUserAgent.class})
@SuppressWarnings("WeakerAccess")
public class TestDeviceEnrichUserAgent {
    
    @Mock
    private UADetectorServiceFactory uaDetectorServiceFactory;

    @InjectMocks
    private DeviceEnrichUserAgent deviceUserAgent;

    TestRunner testRunner;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        deviceUserAgent = new DeviceEnrichUserAgent();
        testRunner = TestRunners.newTestRunner(deviceUserAgent);
    }

    @Test
    public void verifyNonExistentIpFlowsToNotFoundRelationship() throws Exception {
        testRunner.setProperty(DeviceEnrichUserAgent.USER_AGENT_PARSER, DeviceEnrichUserAgent.RESOURCE_MODULE_PARSER);
        testRunner.setProperty(DeviceEnrichUserAgent.USER_AGENT_ATTRIBUTE, "user_agent");

        testRunner.enqueue(new byte[0], Collections.emptyMap());

        testRunner.run();

        List<MockFlowFile> notFound = testRunner.getFlowFilesForRelationship(DeviceEnrichUserAgent.REL_NOT_FOUND);
        List<MockFlowFile> found = testRunner.getFlowFilesForRelationship(DeviceEnrichUserAgent.REL_FOUND);

        assertEquals(1, notFound.size());
        assertEquals(0, found.size());
    }

    @Test
    public void successfulDeviceParseShouldFlowToFoundRelationship() throws Exception {
        testRunner.setProperty(DeviceEnrichUserAgent.USER_AGENT_PARSER, DeviceEnrichUserAgent.RESOURCE_MODULE_PARSER);
        testRunner.setProperty(DeviceEnrichUserAgent.USER_AGENT_ATTRIBUTE, "user_agent");

        DeviceEnrichUserAgent processor = (DeviceEnrichUserAgent)testRunner.getProcessor();

        final ReadableUserAgent result = new UserAgent(
            "Amazo",
            new DeviceCategory(ReadableDeviceCategory.Category.PERSONAL_COMPUTER),
            UserAgentFamily.FIREFOX,
            new OperatingSystem(
                 OperatingSystemFamily.ANDROID,
                 "Android",
                 "ico.png",
                 "iVo",
                 "Professor Ivo",
                 "www.IvyUniversity.edu",
                 "www.android.com",
                 new VersionNumber("0","6","9","2")
            ),
            new VersionNumber("1","2","3","4"),
            UserAgentType.MOBILE_BROWSER
        );

        UserAgentStringParser parser = mock(UserAgentStringParser.class);
        when(parser.parse("agent")).thenReturn(result);

        mockStatic(UADetectorServiceFactory.class);
        // when(UADetectorServiceFactory.getResourceModuleParser()).thenReturn();

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("user_agent", "agent");

        testRunner.enqueue(new byte[0], attributes);

        testRunner.run();

        List<MockFlowFile> notFound = testRunner.getFlowFilesForRelationship(DeviceEnrichUserAgent.REL_NOT_FOUND);
        List<MockFlowFile> found = testRunner.getFlowFilesForRelationship(DeviceEnrichUserAgent.REL_FOUND);

        assertEquals(0, notFound.size());
        assertEquals(1, found.size());

        FlowFile finishedFound = found.get(0);
        assertNotNull(finishedFound.getAttribute("user_agent.device.lookup.micros"));
        
        // assertEquals("Minneapolis", finishedFound.getAttribute("ip.geo.city"));
        // assertEquals("44.98", finishedFound.getAttribute("ip.geo.latitude"));
        // assertEquals("93.2636", finishedFound.getAttribute("ip.geo.longitude"));
        // assertEquals("Minnesota", finishedFound.getAttribute("ip.geo.subdivision.0"));
        // assertEquals("MN", finishedFound.getAttribute("ip.geo.subdivision.isocode.0"));
        // assertNull(finishedFound.getAttribute("ip.geo.subdivision.1"));
        // assertEquals("TT", finishedFound.getAttribute("ip.geo.subdivision.isocode.1"));
        // assertEquals("United States of America", finishedFound.getAttribute("ip.geo.country"));
        // assertEquals("US", finishedFound.getAttribute("ip.geo.country.isocode"));
        // assertEquals("55401", finishedFound.getAttribute("ip.geo.postalcode"));
    }

    private class DeviceCategory implements ReadableDeviceCategory {
        private ReadableDeviceCategory.Category category;
        DeviceCategory(ReadableDeviceCategory.Category _category) {
            category = _category;
        }
        public ReadableDeviceCategory.Category getCategory() {
            return category;
        }
        public String getName() {
            return category.getName();
        }
        public String getIcon() {
            return "";
        }
        public String getInfoUrl() {
            return "";
        }
    }

    private class UserAgent implements ReadableUserAgent {
        private String name;
        private ReadableDeviceCategory category;
        private UserAgentFamily family;
        private OperatingSystem os;
        private VersionNumber version;
        private UserAgentType type;
        
        UserAgent(
            String _name,
            ReadableDeviceCategory _category,
            UserAgentFamily _family,
            OperatingSystem _os,
            VersionNumber _version,
            UserAgentType _type) {
            name = _name;
            category = _category;
            family = _family;
            os = _os;
            version = _version;
            type = _type;
        }

        public ReadableDeviceCategory getDeviceCategory() { return category; }
        public UserAgentFamily getFamily() { return family; }
        public VersionNumber getVersionNumber() { return version; }
        public OperatingSystem getOperatingSystem() { return os; }
        public UserAgentType getType() { return type; }
        public String getTypeName() { return type.getName(); }
        public String getProducer() { return ""; }
        public String getUrl() { return ""; }
        public String getProducerUrl(){ return ""; }
        public String getName() { return name; }
        public String getIcon() { return ""; }
    }

}
