import java.net.InetAddress;

public class Ip {
    public static void main(String[] args) throws Exception {
        System.out.println(InetAddress.getLocalHost().getHostAddress());
    }
}