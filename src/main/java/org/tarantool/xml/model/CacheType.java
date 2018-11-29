
package org.tarantool.xml.model;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlIDREF;
import javax.xml.bind.annotation.XmlSchemaType;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for cache-type complex type.
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "cache-type")
public class CacheType
    extends BaseCacheType
{

    @XmlAttribute(name = "alias", required = true)
    protected String alias;
    @XmlAttribute(name = "uses-template")
    @XmlIDREF
    @XmlSchemaType(name = "IDREF")
    protected Object usesTemplate;

    /**
     * Gets the value of the alias property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getAlias() {
        return alias;
    }

    /**
     * Sets the value of the alias property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setAlias(String value) {
        this.alias = value;
    }

    /**
     * Gets the value of the usesTemplate property.
     * 
     * @return
     *     possible object is
     *     {@link Object }
     *     
     */
    public Object getUsesTemplate() {
        return usesTemplate;
    }

    /**
     * Sets the value of the usesTemplate property.
     * 
     * @param value
     *     allowed object is
     *     {@link Object }
     *     
     */
    public void setUsesTemplate(Object value) {
        this.usesTemplate = value;
    }

}
