/*
 * Copyright Terracotta, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.tarantool.xml;

import org.tarantool.xml.exceptions.XmlConfigurationException;
import org.tarantool.xml.model.BaseCacheType;
import org.tarantool.xml.model.CacheLoaderWriterType;
import org.tarantool.xml.model.CacheTemplateType;
import org.tarantool.xml.model.CacheType;
import org.tarantool.xml.model.ConfigType;
import org.tarantool.xml.model.ConnectionType;
import org.tarantool.xml.model.CopierType;
import org.tarantool.xml.model.EventFiringType;
import org.tarantool.xml.model.EventOrderingType;
import org.tarantool.xml.model.EventType;
import org.tarantool.xml.model.ExpiryType;
import org.tarantool.xml.model.ListenersType;
import org.tarantool.xml.model.MemoryUnit;
import org.tarantool.xml.model.SerializerType;
import org.tarantool.xml.model.ServiceType;
import org.tarantool.xml.model.ThreadPoolReferenceType;
import org.tarantool.xml.model.ThreadPoolsType;
import org.tarantool.xml.model.TimeType;

import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Provides support for parsing a cache configuration expressed in XML.
 */
class ConfigurationParser {

  private static final Pattern SYSPROP = Pattern.compile("\\$\\{([^}]+)\\}");

  private static final String CORE_SCHEMA_ROOT_ELEMENT = "config";
  private static final String CORE_SCHEMA_JAXB_MODEL_PACKAGE = ConfigType.class.getPackage().getName();

  private final Unmarshaller unmarshaller;
  private final ConfigType config;

  static String replaceProperties(String originalValue, final Properties properties) {
    Matcher matcher = SYSPROP.matcher(originalValue);

    StringBuffer sb = new StringBuffer();
    while (matcher.find()) {
      final String property = matcher.group(1);
      final String value = properties.getProperty(property);
      if (value == null) {
        throw new IllegalStateException(String.format("Replacement for ${%s} not found!", property));
      }
      matcher.appendReplacement(sb, Matcher.quoteReplacement(value));
    }
    matcher.appendTail(sb);
    final String resolvedValue = sb.toString();
    return resolvedValue.equals(originalValue) ? null : resolvedValue;
  }

  public ConfigurationParser(String xml) throws IOException, SAXException, JAXBException, ParserConfigurationException {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(true);
    factory.setIgnoringComments(true);
    factory.setIgnoringElementContentWhitespace(true);

    DocumentBuilder domBuilder = factory.newDocumentBuilder();
    domBuilder.setErrorHandler(new FatalErrorHandler());
    Element dom = domBuilder.parse(xml).getDocumentElement();

    substituteSystemProperties(dom);

    if (!CORE_SCHEMA_ROOT_ELEMENT.equals(dom.getLocalName())) {
      throw new XmlConfigurationException("Expecting " + CORE_SCHEMA_ROOT_ELEMENT
          + " element; found " + dom.getLocalName());
    }

    Class<ConfigType> configTypeClass = ConfigType.class;
    JAXBContext jc = JAXBContext.newInstance(CORE_SCHEMA_JAXB_MODEL_PACKAGE, configTypeClass.getClassLoader());
    this.unmarshaller = jc.createUnmarshaller();
    this.config = unmarshaller.unmarshal(dom, configTypeClass).getValue();
  }

  private void substituteSystemProperties(final Element dom) {
    final Properties properties = System.getProperties();
    Stack<NodeList> nodeLists = new Stack<>();
    nodeLists.push(dom.getChildNodes());
    while (!nodeLists.isEmpty()) {
      NodeList nodeList = nodeLists.pop();
      for (int i = 0; i < nodeList.getLength(); ++i) {
        Node currentNode = nodeList.item(i);
        if (currentNode.hasChildNodes()) {
          nodeLists.push(currentNode.getChildNodes());
        }
        final NamedNodeMap attributes = currentNode.getAttributes();
        if (attributes != null) {
          for (int j = 0; j < attributes.getLength(); ++j) {
            final Node attributeNode = attributes.item(j);
            final String newValue = replaceProperties(attributeNode.getNodeValue(), properties);
            if (newValue != null) {
              attributeNode.setNodeValue(newValue);
            }
          }
        }
        if (currentNode.getNodeType() == Node.TEXT_NODE) {
          final String newValue = replaceProperties(currentNode.getNodeValue(), properties);
          if (newValue != null) {
            currentNode.setNodeValue(newValue);
          }
        }
      }
    }
  }

  public Iterable<ServiceType> getServiceElements() {
    return config.getService();
  }

  public SerializerType getDefaultSerializers() {
    return config.getDefaultSerializers();
  }

  public CopierType getDefaultCopiers() {
    return config.getDefaultCopiers();
  }

  public ThreadPoolReferenceType getEventDispatch() {
    return config.getEventDispatch();
  }

  public ThreadPoolReferenceType getWriteBehind() {
    return config.getWriteBehind();
  }

  public ThreadPoolsType getThreadPools() {
    return config.getThreadPools();
  }

  public List<ConnectionType> getConnections() {
    return config.getConnections();
  }

  public CacheTemplate getDefaultTemplate() {
    final CacheTemplateType cacheTemplate = (CacheTemplateType)config.getDefaultTemplate();
    return (cacheTemplate == null) ? null : new CacheTemplate() {

      @Override
      public String keyType() {
        String keyType = cacheTemplate.getKeyType() != null ? cacheTemplate.getKeyType().getValue() : null;
        if (keyType == null) {
          keyType = JaxbHelper.findDefaultValue(cacheTemplate, "keyType");
        }
        return keyType;
      }

      @Override
      public String keySerializer() {
        return cacheTemplate.getKeyType() != null ? cacheTemplate.getKeyType().getSerializer() : null;
      }

      @Override
      public String keyCopier() {
        return cacheTemplate.getKeyType() != null ? cacheTemplate.getKeyType().getCopier() : null;
      }

      @Override
      public String valueType() {
        String valueType = cacheTemplate.getValueType() != null ? cacheTemplate.getValueType().getValue() : null;
        if (valueType == null) {
          valueType = JaxbHelper.findDefaultValue(cacheTemplate, "valueType");
        }
        return valueType;
      }

      @Override
      public String valueSerializer() {
        return cacheTemplate.getValueType() != null ? cacheTemplate.getValueType().getSerializer() : null;
      }

      @Override
      public String valueCopier() {
        return cacheTemplate.getValueType() != null ? cacheTemplate.getValueType().getCopier() : null;
      }

      @Override
      public String evictionAdvisor() {
        return cacheTemplate.getEvictionAdvisor();
      }

      @Override
      public Expiry expiry() {
        ExpiryType cacheTemplateExpiry = cacheTemplate.getExpiry();
        if (cacheTemplateExpiry != null) {
          return new XmlExpiry(cacheTemplateExpiry);
        } else {
          return null;
        }
      }

      @Override
      public ListenersConfig listenersConfig() {
        final ListenersType integration = cacheTemplate.getListeners();
        return integration != null ? new XmlListenersConfig(integration) : null;
      }

      @Override
      public String loaderWriter() {
        final CacheLoaderWriterType loaderWriter = cacheTemplate.getLoaderWriter();
        return loaderWriter != null ? loaderWriter.getClazz() : null;
      }

      @Override
      public String resilienceStrategy() {
        return cacheTemplate.getResilience();
      }

      @Override
      public Boolean enableManagement() {
        return cacheTemplate.getEnableManagement();
      }

      @Override
      public Boolean enableStatistics() {
        return cacheTemplate.getEnableStatistics();
      }

      @Override
      public WriteBehind writeBehind() {
        final CacheLoaderWriterType loaderWriter = cacheTemplate.getLoaderWriter();
        final CacheLoaderWriterType.WriteBehind writebehind = loaderWriter != null ? loaderWriter.getWriteBehind(): null;
        return writebehind != null ? new XmlWriteBehind(writebehind) : null;
      }
    };
  }

  public Iterable<CacheDefinition> getCacheElements() {
    List<CacheDefinition> cacheCfgs = new ArrayList<>();
    final List<BaseCacheType> cacheOrCacheTemplate = config.getCacheOrCacheTemplate();
    for (BaseCacheType baseCacheType : cacheOrCacheTemplate) {
      if(baseCacheType instanceof CacheType) {
        final CacheType cacheType = (CacheType)baseCacheType;

        final BaseCacheType[] sources;
        if(cacheType.getUsesTemplate() != null) {
          sources = new BaseCacheType[2];
          sources[0] = cacheType;
          sources[1] = (BaseCacheType) cacheType.getUsesTemplate();
        } else {
          sources = new BaseCacheType[1];
          sources[0] = cacheType;
        }

        cacheCfgs.add(new CacheDefinition() {
          @Override
          public String id() {
            return cacheType.getAlias();
          }

          @Override
          public String keyType() {
            String value = null;
            for (BaseCacheType source : sources) {
              value = source.getKeyType() != null ? source.getKeyType().getValue() : null;
              if (value != null) break;
            }
            if (value == null) {
              for (BaseCacheType source : sources) {
                value = JaxbHelper.findDefaultValue(source, "keyType");
                if (value != null) break;
              }
            }
            return value;
          }

          @Override
          public String keySerializer() {
            String value = null;
            for (BaseCacheType source : sources) {
              value = source.getKeyType() != null ? source.getKeyType().getSerializer() : null;
              if (value != null) break;
            }
            return value;
          }

          @Override
          public String keyCopier() {
            String value = null;
            for (BaseCacheType source : sources) {
              value = source.getKeyType() != null ? source.getKeyType().getCopier() : null;
              if (value != null) break;
            }
            return value;
          }

          @Override
          public String valueType() {
            String value = null;
            for (BaseCacheType source : sources) {
              value = source.getValueType() != null ? source.getValueType().getValue() : null;
              if (value != null) break;
            }
            if (value == null) {
              for (BaseCacheType source : sources) {
                value = JaxbHelper.findDefaultValue(source, "valueType");
                if (value != null) break;
              }
            }
            return value;
          }

          @Override
          public String valueSerializer() {
            String value = null;
            for (BaseCacheType source : sources) {
              value = source.getValueType() != null ? source.getValueType().getSerializer() : null;
              if (value != null) break;
            }
            return value;
          }

          @Override
          public String valueCopier() {
            String value = null;
            for (BaseCacheType source : sources) {
              value = source.getValueType() != null ? source.getValueType().getCopier() : null;
              if (value != null) break;
            }
            return value;
          }

          @Override
          public String evictionAdvisor() {
            String value = null;
            for (BaseCacheType source : sources) {
              value = source.getEvictionAdvisor();
              if (value != null) break;
            }
            return value;
          }

          @Override
          public Expiry expiry() {
            ExpiryType value = null;
            for (BaseCacheType source : sources) {
              value = source.getExpiry();
              if (value != null) break;
            }
            if (value != null) {
              return new XmlExpiry(value);
            } else {
              return null;
            }
          }

          @Override
          public String loaderWriter() {
            String configClass = null;
            for (BaseCacheType source : sources) {
              final CacheLoaderWriterType loaderWriter = source.getLoaderWriter();
              if (loaderWriter != null) {
                configClass = loaderWriter.getClazz();
                break;
              }
            }
            return configClass;
          }

          @Override
          public String resilienceStrategy() {
            for (BaseCacheType source : sources) {
              String resilienceClass = source.getResilience();
              if (resilienceClass != null) {
                return resilienceClass;
              }
            }
            return null;
          }

          @Override
          public Boolean enableManagement() {
            for (BaseCacheType source : sources) {
              Boolean result = source.getEnableManagement();
              if (result != null) {
                return result;
              }
            }
            return false;
          }

          @Override
          public Boolean enableStatistics() {
            for (BaseCacheType source : sources) {
              Boolean result = source.getEnableStatistics();
              if (result != null) {
                return result;
              }
            }
            return false;
          }

          @Override
          public ListenersConfig listenersConfig() {
            ListenersType base = null;
            ArrayList<ListenersType> additionals = new ArrayList<>();
            for (BaseCacheType source : sources) {
              if (source.getListeners() != null) {
                if (base == null) {
                  base = source.getListeners();
                } else {
                  additionals.add(source.getListeners());
                }
              }
            }
            return base != null ? new XmlListenersConfig(base, additionals.toArray(new ListenersType[0])) : null;
          }

          @Override
          public WriteBehind writeBehind() {
            for (BaseCacheType source : sources) {
              final CacheLoaderWriterType loaderWriter = source.getLoaderWriter();
              final CacheLoaderWriterType.WriteBehind writebehind = loaderWriter != null ? loaderWriter.getWriteBehind() : null;
              if (writebehind != null) {
                return new XmlWriteBehind(writebehind);
              }
            }
            return null;
          }
        });
      }
    }

    return Collections.unmodifiableList(cacheCfgs);
  }

  public Map<String, CacheTemplate> getTemplates() {
    final Map<String, CacheTemplate> templates = new HashMap<>();
    final List<BaseCacheType> cacheOrCacheTemplate = config.getCacheOrCacheTemplate();
    for (BaseCacheType baseCacheType : cacheOrCacheTemplate) {
      if (baseCacheType instanceof CacheTemplateType) {
        final CacheTemplateType cacheTemplate = (CacheTemplateType)baseCacheType;
        templates.put(cacheTemplate.getName(), new CacheTemplate() {

          @Override
          public String keyType() {
            String keyType = cacheTemplate.getKeyType() != null ? cacheTemplate.getKeyType().getValue() : null;
            if (keyType == null) {
              keyType = JaxbHelper.findDefaultValue(cacheTemplate, "keyType");
            }
            return keyType;
          }

          @Override
          public String keySerializer() {
            return cacheTemplate.getKeyType() != null ? cacheTemplate.getKeyType().getSerializer() : null;
          }

          @Override
          public String keyCopier() {
            return cacheTemplate.getKeyType() != null ? cacheTemplate.getKeyType().getCopier() : null;
          }

          @Override
          public String valueType() {
            String valueType = cacheTemplate.getValueType() != null ? cacheTemplate.getValueType().getValue() : null;
            if (valueType == null) {
              valueType = JaxbHelper.findDefaultValue(cacheTemplate, "valueType");
            }
            return valueType;
          }

          @Override
          public String valueSerializer() {
            return cacheTemplate.getValueType() != null ? cacheTemplate.getValueType().getSerializer() : null;
          }

          @Override
          public String valueCopier() {
            return cacheTemplate.getValueType() != null ? cacheTemplate.getValueType().getCopier() : null;
          }

          @Override
          public String evictionAdvisor() {
            return cacheTemplate.getEvictionAdvisor();
          }

          @Override
          public Expiry expiry() {
            ExpiryType cacheTemplateExpiry = cacheTemplate.getExpiry();
            if (cacheTemplateExpiry != null) {
              return new XmlExpiry(cacheTemplateExpiry);
            } else {
              return null;
            }
          }

          @Override
          public ListenersConfig listenersConfig() {
            final ListenersType integration = cacheTemplate.getListeners();
            return integration != null ? new XmlListenersConfig(integration) : null;
          }

          @Override
          public String loaderWriter() {
            final CacheLoaderWriterType loaderWriter = cacheTemplate.getLoaderWriter();
            return loaderWriter != null ? loaderWriter.getClazz() : null;
          }

          @Override
          public String resilienceStrategy() {
            return cacheTemplate.getResilience();
          }

          @Override
          public Boolean enableManagement() {
            return cacheTemplate.getEnableManagement();
          }

          @Override
          public Boolean enableStatistics() {
            return cacheTemplate.getEnableStatistics();
          }

          @Override
          public WriteBehind writeBehind() {
            final CacheLoaderWriterType loaderWriter = cacheTemplate.getLoaderWriter();
            final CacheLoaderWriterType.WriteBehind writebehind = loaderWriter != null ? loaderWriter.getWriteBehind(): null;
            return writebehind != null ? new XmlWriteBehind(writebehind) : null;
          }
        });
      }
    }
    return Collections.unmodifiableMap(templates);
  }



  static class FatalErrorHandler implements ErrorHandler {

    @Override
    public void warning(SAXParseException exception) throws SAXException {
      throw exception;
    }

    @Override
    public void error(SAXParseException exception) throws SAXException {
      throw exception;
    }

    @Override
    public void fatalError(SAXParseException exception) throws SAXException {
      throw exception;
    }
  }

  interface CacheTemplate {

    String keyType();

    String keySerializer();

    String keyCopier();

    String valueType();

    String valueSerializer();

    String valueCopier();

    String evictionAdvisor();

    Expiry expiry();

    String loaderWriter();

    String resilienceStrategy();

    Boolean enableManagement();

    Boolean enableStatistics();

    ListenersConfig listenersConfig();

    WriteBehind writeBehind();

  }

  interface CacheDefinition extends CacheTemplate {

    String id();

  }

  interface ListenersConfig {

    int dispatcherConcurrency();

    String threadPool();

    Iterable<Listener> listeners();
  }

  interface Listener {

    String className();

    EventFiringType eventFiring();

    EventOrderingType eventOrdering();

    List<EventType> fireOn();

  }

  interface Expiry {

    boolean isUserDef();

    boolean isTTI();

    boolean isTTL();

    String type();

    long value();

    TimeUnit unit();

  }

  interface WriteBehind {

    int maxQueueSize();

    int concurrency();

    String threadPool();

    Batching batching();
  }

  interface Batching {

    boolean isCoalesced();

    int batchSize();

    long maxDelay();

    TimeUnit maxDelayUnit();
  }

  interface SizeOfEngineLimits {

    long getMaxObjectGraphSize();

    long getMaxObjectSize();

    MemoryUnit getUnit();
  }

  interface ResilienceStrategy {

  }
  private static class XmlListenersConfig implements ListenersConfig {

    final int dispatcherConcurrency;
    final String threadPool;
    final Iterable<Listener> listeners;

    private XmlListenersConfig(final ListenersType type, final ListenersType... others) {
      this.dispatcherConcurrency = type.getDispatcherConcurrency().intValue();
      String threadPool = type.getDispatcherThreadPool();
      Set<Listener> listenerSet = new HashSet<>();
      final List<ListenersType.Listener> xmlListeners = type.getListener();
      extractListeners(listenerSet, xmlListeners);

      for (ListenersType other : others) {
        if (threadPool == null && other.getDispatcherThreadPool() != null) {
          threadPool = other.getDispatcherThreadPool();
        }
        extractListeners(listenerSet, other.getListener());
      }

      this.threadPool = threadPool;
      this.listeners = !listenerSet.isEmpty() ? listenerSet : null;
    }

    private void extractListeners(Set<Listener> listenerSet, List<ListenersType.Listener> xmlListeners) {
      if(xmlListeners != null) {
        for(final ListenersType.Listener listener : xmlListeners) {
          listenerSet.add(new Listener() {
            @Override
            public String className() {
              return listener.getClazz();
            }

            @Override
            public EventFiringType eventFiring() {
              return listener.getEventFiringMode();
            }

            @Override
            public EventOrderingType eventOrdering() {
              return listener.getEventOrderingMode();
            }

            @Override
            public List<EventType> fireOn() {
              return listener.getEventsToFireOn();
            }
          });
        }
      }
    }

    @Override
    public int dispatcherConcurrency() {
      return dispatcherConcurrency;
    }

    @Override
    public String threadPool() {
      return threadPool;
    }

    @Override
    public Iterable<Listener> listeners() {
      return listeners;
    }

  }

  private static class XmlExpiry implements Expiry {

    final ExpiryType type;

    private XmlExpiry(final ExpiryType type) {
      this.type = type;
    }

    @Override
    public boolean isUserDef() {
      return type != null && type.getClazz() != null;
    }

    @Override
    public boolean isTTI() {
      return type != null && type.getTti() != null;
    }

    @Override
    public boolean isTTL() {
      return type != null && type.getTtl() != null;
    }

    @Override
    public String type() {
      return type.getClazz();
    }

    @Override
    public long value() {
      final TimeType time;
      if(isTTI()) {
        time = type.getTti();
      } else {
        time = type.getTtl();
      }
      return time == null ? 0L : time.getValue().longValue();
    }

    @Override
    public TimeUnit unit() {
      final TimeType time;
      if(isTTI()) {
        time = type.getTti();
      } else {
        time = type.getTtl();
      }
      if(time != null) {
        return XmlModel.convertToJUCTimeUnit(time.getUnit());
      }
      return null;
    }
  }

  private static class XmlWriteBehind implements WriteBehind {

    private final CacheLoaderWriterType.WriteBehind writebehind;

    private XmlWriteBehind(CacheLoaderWriterType.WriteBehind writebehind) {
      this.writebehind = writebehind;
    }

    @Override
    public int maxQueueSize() {
      return this.writebehind.getSize().intValue();
    }

    @Override
    public int concurrency() {
      return this.writebehind.getConcurrency().intValue() ;
    }

    @Override
    public String threadPool() {
      return this.writebehind.getThreadPool();
    }

    @Override
    public Batching batching() {
      CacheLoaderWriterType.WriteBehind.Batching batching = writebehind.getBatching();
      if (batching == null) {
        return null;
      } else {
        return new XmlBatching(batching);
      }
    }

  }

  private static class XmlBatching implements Batching {

    private final CacheLoaderWriterType.WriteBehind.Batching batching;

    private XmlBatching(CacheLoaderWriterType.WriteBehind.Batching batching) {
      this.batching = batching;
    }

    @Override
    public boolean isCoalesced() {
      return this.batching.isCoalesce();
    }

    @Override
    public int batchSize() {
      return this.batching.getBatchSize().intValue();
    }

    @Override
    public long maxDelay() {
      return this.batching.getMaxWriteDelay().getValue().longValue();
    }

    @Override
    public TimeUnit maxDelayUnit() {
      return XmlModel.convertToJUCTimeUnit(this.batching.getMaxWriteDelay().getUnit());
    }

  }

}
