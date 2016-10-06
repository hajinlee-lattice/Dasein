import javax.crypto.Cipher;

class Test {
  public static void main(String[] args) {
    try {
      int maxKeyLen = Cipher.getMaxAllowedKeyLength("AES");
      if (maxKeyLen == 2147483647) {
        System.out.println("JCE is installed");
        return;
      }
      System.out.println("JCE is not installed!");
    } catch (Exception e){
      System.out.println(e);
      return;
    }
  }
}
