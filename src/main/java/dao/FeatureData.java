package dao;

import java.io.Serializable;

/**
 * Class representing features data used by the machine learning algorithm.
 */
public class FeatureData implements Serializable {

    private long imei;
    private int height;
    private int weight;
    private int age;
    private int gender;
    private double meanX;
    private double meanY;
    private double meanZ;
    private double varianceX;
    private double varianceY;
    private double varianceZ;
    private double avgAbsDiffX;
    private double avgAbsDiffY;
    private double avgAbsDiffZ;
    private double resultant;
    private double avgTimePeak;
    private int activity;
    private long timestampStart;
    private long timestampStop;

    public FeatureData() {
    }

    public FeatureData(long imei, int height, int weight, int age, int gender, double meanX, double meanY, double meanZ, double varianceX, double varianceY, double varianceZ, double avgAbsDiffX, double avgAbsDiffY, double avgAbsDiffZ, double resultant, double avgTimePeak, int activity) {
        this.imei = imei;
        this.height = height;
        this.weight = weight;
        this.age = age;
        this.gender = gender;
        this.meanX = meanX;
        this.meanY = meanY;
        this.meanZ = meanZ;
        this.varianceX = varianceX;
        this.varianceY = varianceY;
        this.varianceZ = varianceZ;
        this.avgAbsDiffX = avgAbsDiffX;
        this.avgAbsDiffY = avgAbsDiffY;
        this.avgAbsDiffZ = avgAbsDiffZ;
        this.resultant = resultant;
        this.avgTimePeak = avgTimePeak;
        this.activity = activity;
    }

    public long getImei() {
        return imei;
    }

    public void setImei(long imei) {
        this.imei = imei;
    }

    public int getHeight() {
        return height;
    }

    public void setHeight(int height) {
        this.height = height;
    }

    public int getWeight() {
        return weight;
    }

    public void setWeight(int weight) {
        this.weight = weight;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public int getGender() {
        return gender;
    }

    public void setGender(int gender) {
        this.gender = gender;
    }

    public double getMeanX() {
        return meanX;
    }

    public void setMeanX(double meanX) {
        this.meanX = meanX;
    }

    public double getMeanY() {
        return meanY;
    }

    public void setMeanY(double meanY) {
        this.meanY = meanY;
    }

    public double getMeanZ() {
        return meanZ;
    }

    public void setMeanZ(double meanZ) {
        this.meanZ = meanZ;
    }

    public double getVarianceX() {
        return varianceX;
    }

    public void setVarianceX(double varianceX) {
        this.varianceX = varianceX;
    }

    public double getVarianceY() {
        return varianceY;
    }

    public void setVarianceY(double varianceY) {
        this.varianceY = varianceY;
    }

    public double getVarianceZ() {
        return varianceZ;
    }

    public void setVarianceZ(double varianceZ) {
        this.varianceZ = varianceZ;
    }

    public double getAvgAbsDiffX() {
        return avgAbsDiffX;
    }

    public void setAvgAbsDiffX(double avgAbsDiffX) {
        this.avgAbsDiffX = avgAbsDiffX;
    }

    public double getAvgAbsDiffY() {
        return avgAbsDiffY;
    }

    public void setAvgAbsDiffY(double avgAbsDiffY) {
        this.avgAbsDiffY = avgAbsDiffY;
    }

    public double getAvgAbsDiffZ() {
        return avgAbsDiffZ;
    }

    public void setAvgAbsDiffZ(double avgAbsDiffZ) {
        this.avgAbsDiffZ = avgAbsDiffZ;
    }

    public double getResultant() {
        return resultant;
    }

    public void setResultant(double resultant) {
        this.resultant = resultant;
    }

    public double getAvgTimePeak() {
        return avgTimePeak;
    }

    public void setAvgTimePeak(double avgTimePeak) {
        this.avgTimePeak = avgTimePeak;
    }

    public int getActivity() {
        return activity;
    }

    public void setActivity(int activity) {
        this.activity = activity;
    }

    public long getTimestampStart() {
        return timestampStart;
    }

    public void setTimestampStart(long timestampStart) {
        this.timestampStart = timestampStart;
    }

    public long getTimestampStop() {
        return timestampStop;
    }

    public void setTimestampStop(long timestampStop) {
        this.timestampStop = timestampStop;
    }
}
