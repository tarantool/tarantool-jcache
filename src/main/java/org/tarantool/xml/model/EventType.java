
package org.tarantool.xml.model;

import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for event-type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * <p>
 * <pre>
 * &lt;simpleType name="event-type"&gt;
 *   &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string"&gt;
 *     &lt;enumeration value="EVICTED"/&gt;
 *     &lt;enumeration value="EXPIRED"/&gt;
 *     &lt;enumeration value="REMOVED"/&gt;
 *     &lt;enumeration value="CREATED"/&gt;
 *     &lt;enumeration value="UPDATED"/&gt;
 *   &lt;/restriction&gt;
 * &lt;/simpleType&gt;
 * </pre>
 * 
 */
@XmlType(name = "event-type")
@XmlEnum
public enum EventType {

    EVICTED,
    EXPIRED,
    REMOVED,
    CREATED,
    UPDATED;

    public String value() {
        return name();
    }

    public static EventType fromValue(String v) {
        return valueOf(v);
    }

}
