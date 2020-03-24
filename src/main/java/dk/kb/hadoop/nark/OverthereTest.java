package dk.kb.hadoop.nark;

import static com.xebialabs.overthere.ConnectionOptions.ADDRESS;
import static com.xebialabs.overthere.ConnectionOptions.OPERATING_SYSTEM;
import static com.xebialabs.overthere.ConnectionOptions.PASSWORD;
import static com.xebialabs.overthere.ConnectionOptions.USERNAME;
import static com.xebialabs.overthere.OperatingSystemFamily.UNIX;
import static com.xebialabs.overthere.ssh.SshConnectionBuilder.CONNECTION_TYPE;
import static com.xebialabs.overthere.ssh.SshConnectionType.SCP;

import com.xebialabs.overthere.*;

import java.io.*;

public class OverthereTest {
    public static void main(String[] args) {

        ConnectionOptions options = new ConnectionOptions();
        options.set(ADDRESS, "node1");
        options.set(USERNAME, "vagrant");
        options.set(PASSWORD, "vagrant");
        options.set(OPERATING_SYSTEM, UNIX);
        options.set(CONNECTION_TYPE, SCP);
        try (OverthereConnection connection = Overthere.getConnection("ssh", options)) {
            copyLocalToRemote(connection, "./target/hadoopifications-1.0-SNAPSHOT-withdeps.jar", "/home/vagrant/test.jar");

            copyLocalToRemote(connection, "./src/test/resources/input", "/home/vagrant/input");

            CmdLine cmd = new CmdLine();
            cmd.addRaw("java -cp /home/vagrant/test.jar " +
                    "dk.kb.hadoop.nark.CDXJob " +
                    "file:///home/vagrant/input " +
                    "output");
            connection.execute(cmd); // Starts the hadoop job
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void copyLocalToRemote(OverthereConnection connection, String localPath, String remotePath) throws IOException {
        File localFile = new File(localPath); // Local path
        OverthereFile remoteFile = connection.getFile(remotePath); // Remote path
        BufferedInputStream bis = null;
        BufferedOutputStream bos = null;
        byte[] bucket = new byte[32 * 1024];

        if (localFile.isFile()) {
            bis = new BufferedInputStream(new FileInputStream(localFile));
            bos = new BufferedOutputStream(remoteFile.getOutputStream());
            int bytesRead = 0;
            while (bytesRead != -1) {
                bytesRead = bis.read(bucket); //-1, 0, or more
                if (bytesRead > 0) {
                    bos.write(bucket, 0, bytesRead);
                }
            }
        } else { // else it's a directory
            final File[] files = localFile.listFiles();
            remoteFile.mkdir(); // Not sure if necessary
            for (File file : files) {
                String name = file.getName();
                bis = new BufferedInputStream(new FileInputStream(file));
                bos = new BufferedOutputStream(remoteFile.getFile(name).getOutputStream());
                int bytesRead = 0;
                while (bytesRead != -1) {
                    bytesRead = bis.read(bucket); //-1, 0, or more
                    if (bytesRead > 0) {
                        bos.write(bucket, 0, bytesRead);
                    }
                }
            }
        }

        bis.close();
        bos.close();
    }
}
