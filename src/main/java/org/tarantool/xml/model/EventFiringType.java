
package org.tarantool.xml.model;

import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for event-firing-type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * <p>
 * <pre>
 * &lt;simpleType name="event-firing-type"&gt;
 *   &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string"&gt;
 *     &lt;enumeration value="ASYNCHRONOUS"/&gt;
 *     &lt;enumeration value="SYNCHRONOUS"/&gt;
 *   &lt;/restriction&gt;
 * &lt;/simpleType&gt;
 * </pre>
 * 
 */
@XmlType(name = "event-firing-type")
@XmlEnum
public enum EventFiringType {

    ASYNCHRONOUS,
    SYNCHRONOUS;

    public String value() {
        return name();
    }

    public static EventFiringType fromValue(String v) {
        return valueOf(v);
    }

}
