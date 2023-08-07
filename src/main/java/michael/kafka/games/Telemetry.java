package michael.kafka.games;

import org.apache.avro.specific.SpecificRecord;

public interface Telemetry extends SpecificRecord {

    java.lang.CharSequence getDeviceControllerSerial();

    java.lang.CharSequence getSiteId();
    void setSiteId(java.lang.CharSequence value);

}
