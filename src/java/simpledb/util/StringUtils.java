package simpledb.util;

public class StringUtils {
  public static String convertByteToHexadecimal(byte[] byteArray) {
    StringBuilder hex = new StringBuilder();

    // Iterating through each byte in the array
    for (byte i : byteArray) {
      hex.append(String.format("%02X", i));
    }
    return hex.toString();
  }
}
