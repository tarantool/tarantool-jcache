
package org.tarantool.xml.model;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAnyElement;
import javax.xml.bind.annotation.XmlType;
import org.w3c.dom.Element;


/**
 * <p>Java class for service-type complex type.
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "service-type", propOrder = {
    "serviceCreationConfiguration"
})
public class ServiceType {

    @XmlAnyElement
    protected Element serviceCreationConfiguration;

    /**
     * Gets the value of the serviceCreationConfiguration property.
     * 
     * @return
     *     possible object is
     *     {@link Element }
     *     
     */
    public Element getServiceCreationConfiguration() {
        return serviceCreationConfiguration;
    }

    /**
     * Sets the value of the serviceCreationConfiguration property.
     * 
     * @param value
     *     allowed object is
     *     {@link Element }
     *     
     */
    public void setServiceCreationConfiguration(Element value) {
        this.serviceCreationConfiguration = value;
    }

}
