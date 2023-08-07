package michael.kafka.games;

public interface Telemetry {

    java.lang.CharSequence getDeviceControllerSerial();

    java.lang.CharSequence getSiteId();
    void setSiteId(java.lang.CharSequence value);

}
