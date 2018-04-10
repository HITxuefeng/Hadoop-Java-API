package com.iie.s3_multi;

public class Int2byteArray {

    public Int2byteArray(){}
    /**
     * int值转成4字节的byte数组
     * @param num
     * @return
     */
    public byte[] i2ba(int num) {
        byte[] result = new byte[4];
        result[0] = (byte)(num >>> 24);//取最高8位放到0下标
        result[1] = (byte)(num >>> 16);//取次高8为放到1下标
        result[2] = (byte)(num >>> 8); //取次低8位放到2下标
        result[3] = (byte)(num );      //取最低8位放到3下标
        return result;
    }

    public static void main(String[] args) {
        Int2byteArray im = new Int2byteArray();
        im.i2ba(10000);
    }
}
