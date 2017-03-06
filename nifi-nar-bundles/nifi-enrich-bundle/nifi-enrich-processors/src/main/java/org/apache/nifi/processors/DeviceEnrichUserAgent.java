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

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.maxmind.DatabaseReader;
import org.apache.nifi.util.StopWatch;

import net.sf.uadetector.service.UADetectorServiceFactory;
import net.sf.uadetector.UserAgent;
import net.sf.uadetector.UserAgentStringParser;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"device", "enrich", "user agent"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Looks up device information for a user agent and adds the device information to FlowFile attributes. The "
        + "device data is provided by http://uadetector.sourceforge.net/. The attribute that contains the user agent to lookup is provided by the "
        + "'User Agent Attribute' property. If the name of the attribute provided is 'X', then the the attributes added by enrichment "
        + "will take the form X.device.<fieldName>")
@WritesAttributes({
    @WritesAttribute(attribute = "X.lookup.micros",        description = "The number of microseconds that the device lookup took"),
    @WritesAttribute(attribute = "X.category",             description = "The category identified for the User Agent"),
    @WritesAttribute(attribute = "X.family",               description = "The family identified for this User Agent"),
    @WritesAttribute(attribute = "X.name",                 description = "The name identified for this User Agent"),
    @WritesAttribute(attribute = "X.producer",             description = "The producer identified for this User Agent"),
    @WritesAttribute(attribute = "X.type",                 description = "The type for this User Agent"),
    @WritesAttribute(attribute = "X.version.full",         description = "The user agent version (full) for this User Agent"),
    @WritesAttribute(attribute = "X.version.major",        description = "The user agent version (major) for this User Agent"),
    @WritesAttribute(attribute = "X.version.minor",        description = "The user agent version (minor) for this User Agent"),
    @WritesAttribute(attribute = "X.version.bugfix",       description = "The user agent version (bugfix) for this User Agent"),
    @WritesAttribute(attribute = "X.version.extension",    description = "The user agent version (extension) for this User Agent"),
    @WritesAttribute(attribute = "X.os.family",            description = "The os family for this User Agent"),
    @WritesAttribute(attribute = "X.os.name",              description = "The os name for this User Agent"),
    @WritesAttribute(attribute = "X.os.producer",          description = "The os producer for this User Agent"),
    @WritesAttribute(attribute = "X.os.version.full",      description = "The os version (full) for this User Agent"),
    @WritesAttribute(attribute = "X.os.version.major",     description = "The os version (major) for this User Agent"),
    @WritesAttribute(attribute = "X.os.version.minor",     description = "The os version (minor) for this User Agent"),
    @WritesAttribute(attribute = "X.os.version.bugfix",    description = "The os version (bugfix) for this User Agent"),
    @WritesAttribute(attribute = "X.os.version.extension", description = "The os version (extension) for this User Agent"),})
public class DeviceEnrichUserAgent extends AbstractProcessor {

    /*
        Returns an implementation of UserAgentStringParser which checks at regular intervals for new versions of UAS data (also known as database).
        When newer data available, it automatically loads and updates it. Additionally the loaded data are stored in a cache file.
        At initialization time the returned parser will be loaded with the UAS data of the cache file. If the cache file doesn't exist or is empty the data of this module will be loaded.
        The initialization is started only when this method is called the first time.

        The update of the data store runs as background task. With this feature we try to reduce the initialization time of this UserAgentStringParser,
        because a network connection is involved and the remote system can be not available or slow.

        The static class definition CachingAndUpdatingParserHolder within this factory class is not initialized until the JVM determines that CachingAndUpdatingParserHolder must be executed.
        The static class CachingAndUpdatingParserHolder is only executed when the static method getOnlineUserAgentStringParser is invoked on the class UADetectorServiceFactory,
        and the first time this happens the JVM will load and initialize the CachingAndUpdatingParserHolder class.

        If during the operation the Internet connection gets lost, then this instance continues to work properly (and under correct log level settings you will get an corresponding log messages).
    */
    public static final CACHING_AND_UPDATING_PARSER = "Caching And Updating Parser";

    /*
        Returns an implementation of UserAgentStringParser which checks at regular intervals for new versions of UAS data (also known as database). When newer data available,
        it automatically loads and updates it.
        At initialization time the returned parser will be loaded with the UAS data of this module (the shipped one within the uadetector-resources JAR) and tries to update it.
        The initialization is started only when this method is called the first time.

        The update of the data store runs as background task. With this feature we try to reduce the initialization time of this UserAgentStringParser,
        because a network connection is involved and the remote system can be not available or slow.

        The static class definition OnlineUpdatingParserHolder within this factory class is not initialized until the JVM determines that OnlineUpdatingParserHolder must be executed.
        The static class OnlineUpdatingParserHolder is only executed when the static method getOnlineUserAgentStringParser is invoked on the class UADetectorServiceFactory,
        and the first time this happens the JVM will load and initialize the OnlineUpdatingParserHolder class.

        If during the operation the Internet connection gets lost, then this instance continues to work properly (and under correct log level settings you will get an corresponding log messages).
    */
    public static final ONLINE_UPDATING_PARSER = "Online Updating Parser";


    /*
        Returns an implementation of UserAgentStringParser with no updating functions. It will be loaded by using the shipped UAS data (also known as database) of this module.
        The database is loaded once during initialization. The initialization is started at class loading of this class (UADetectorServiceFactory).
    */
    public static final RESOURCE_MODULE_PARSER = "Resource Module Parser";

    public static final PropertyDescriptor USER_AGENT_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("User Agent Attribute")
            .required(true)
            .description("The name of an attribute whose value is a user agent for which enrichment should occur")
            .defaultValue("device")
            .addValidator(StandardValidators.ATTRIBUTE_KEY_VALIDATOR)
            .build();

    public static final PropertyDescriptor USER_AGENT_PARSER = new PropertyDescriptor.Builder()
            .name("User Agent Parser")
            .required(true)
            .allowableValues(
                new AllowableValue(CACHING_UPDATE_PARSER, "Parser that checks at regular intervals for new versions of UAS data. "
                    + "When newer data available, it automatically loads and updates it. Additionally the loaded data are stored in a cache file."),
                new AllowableValue(ONLINE_UPDATING_PARSER, "parser that checks at regular intervals for new versions of UAS data. "
                    + "When newer data available, it automatically loads and updates it."),
                new AllowableValue(RESOURCE_MODULE_PARSER, "Parser with no updating functions. "
                    + "The database is loaded once during initialization."))
            .defaultValue(RESOURCE_MODULE_PARSER)
            .description("Whether to enable caching and updating for the parser.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_FOUND = new Relationship.Builder()
            .name("found")
            .description("Where to route flow files after successfully enriching attributes with device data")
            .build();

    public static final Relationship REL_NOT_FOUND = new Relationship.Builder()
            .name("not found")
            .description("Where to route flow files after unsuccessfully enriching attributes because "
                + "no device data was found or no attribute value was present.")
            .build();

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> propertyDescriptors;
    private final AtomicReference<UserAgentStringParser> parserRef = new AtomicReference<>(null);

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @OnScheduled
    public final void onScheduled(final ProcessContext context) throws IOException {
        final StopWatch stopWatch = new StopWatch(true);
        final UserAgentStringParser parser;
        
        final String parserString = context.getProperty(USER_AGENT_PARSER).getValue();
        switch (parserString) {
            case CACHING_AND_UPDATING_PARSER:
                parser = UADetectorServiceFactory.getCachingAndUpdatingParser();
                break;
            case ONLINE_UPDATING_PARSER:
                parser = UADetectorServiceFactory.getOnlineUpdatingParser();
                break;
            case RESOURCE_MODULE_PARSER:
                parser = UADetectorServiceFactory.getResourceModuleParser();
                break;
        }

        stopWatch.stop();
        getLogger().info("Completed loading of UAS Database.  Elapsed time was {} milliseconds.", new Object[]{stopWatch.getDuration(TimeUnit.MILLISECONDS)});
        parserRef.set(parser);
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_FOUND);
        rels.add(REL_NOT_FOUND);
        this.relationships = Collections.unmodifiableSet(rels);

        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(USER_AGENT_ATTRIBUTE);
        props.add(USER_AGENT_PARSER);
        this.propertyDescriptors = Collections.unmodifiableList(props);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final UserAgentStringParser parser = parserRef.get();
        final String userAgentAttributeName = context.getProperty(USER_AGENT_ATTRIBUTE).getValue();
        final String userAgentAttributeValue = flowFile.getAttribute(userAgentAttributeName);
        if (StringUtils.isEmpty(userAgentAttributeName)) {
            session.transfer(flowFile, REL_NOT_FOUND);
            getLogger().warn("Unable to find user agent for {}", new Object[]{flowFile});
            return;
        }

        final StopWatch stopWatch = new StopWatch(true);

        ReadableUserAgent result = parser.parse(userAgentAttributeValue);
        
        stopWatch.stop();

        if (response == null) {
            session.transfer(flowFile, REL_NOT_FOUND);
            return;
        }

        final Map<String, String> attrs = new HashMap<>();
        attrs.put(new StringBuilder(userAgentAttributeName).append(".device.lookup.micros").toString(), String.valueOf(stopWatch.getDuration(TimeUnit.MICROSECONDS)));
        
        attrs.put(new StringBuilder(userAgentAttributeName).append(".device.name").toString(), result.getName());

        ReadableDeviceCategory category = result.getDeviceCategory();

        attrs.put(new StringBuilder(userAgentAttributeName).append(".device.category").toString(), category.getName());

        UserAgentFamily family = result.getFamily();

        attrs.put(new StringBuilder(userAgentAttributeName).append(".device.family").toString(), family.getName());

        attrs.put(new StringBuilder(userAgentAttributeName).append(".device.producer").toString(), result.getProducer());

        attrs.put(new StringBuilder(userAgentAttributeName).append(".device.type").toString(), result.getTypeName());

        VersionNumber deviceVersion = result.getVersionNumber();

        attrs.put(new StringBuilder(userAgentAttributeName).append(".device.version.full").toString(), deviceVersion.toVersionString());
        attrs.put(new StringBuilder(userAgentAttributeName).append(".device.version.major").toString(), deviceVersion.getMajor());
        attrs.put(new StringBuilder(userAgentAttributeName).append(".device.version.minor").toString(), deviceVersion.getMinor());
        attrs.put(new StringBuilder(userAgentAttributeName).append(".device.version.bugfix").toString(), deviceVersion.getBugfix());
        attrs.put(new StringBuilder(userAgentAttributeName).append(".device.version.extension").toString(), deviceVersion.getExtension());
        
        OperatingSystem os = result.getOperatingSystem();

        attrs.put(new StringBuilder(userAgentAttributeName).append(".device.os.family").toString(), os.getFamilyName());
        attrs.put(new StringBuilder(userAgentAttributeName).append(".device.os.name").toString(), os.getName());
        attrs.put(new StringBuilder(userAgentAttributeName).append(".device.os.producer").toString(), os.getProducer());

        VersionNumber osVersion = os.getVersionNumber();

        attrs.put(new StringBuilder(userAgentAttributeName).append(".device.os.version.full").toString(), osVersion.toVersionString());
        attrs.put(new StringBuilder(userAgentAttributeName).append(".device.os.version.major").toString(), osVersion.getMajor());
        attrs.put(new StringBuilder(userAgentAttributeName).append(".device.os.version.minor").toString(), osVersion.getMinor());
        attrs.put(new StringBuilder(userAgentAttributeName).append(".device.os.version.bugfix").toString(), osVersion.getBugfix());
        attrs.put(new StringBuilder(userAgentAttributeName).append(".device.os.version.extension").toString(), osVersion.getExtension());
        
        flowFile = session.putAllAttributes(flowFile, attrs);

        session.transfer(flowFile, REL_FOUND);
    }

}
