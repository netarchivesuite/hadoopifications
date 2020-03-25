package dk.kb.hadoop.nark;

import static com.xebialabs.overthere.ConnectionOptions.ADDRESS;
import static com.xebialabs.overthere.ConnectionOptions.OPERATING_SYSTEM;
import static com.xebialabs.overthere.ConnectionOptions.PASSWORD;
import static com.xebialabs.overthere.ConnectionOptions.USERNAME;
import static com.xebialabs.overthere.OperatingSystemFamily.UNIX;
import static com.xebialabs.overthere.ssh.SshConnectionBuilder.CONNECTION_TYPE;
import static com.xebialabs.overthere.ssh.SshConnectionType.SCP;

import com.xebialabs.overthere.*;
import org.apache.commons.io.FilenameUtils;

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
            scpFile(connection, "./target/hadoopifications-1.0-SNAPSHOT-withdeps.jar", "/home/vagrant/test.jar");
            scpFolder(connection, "./src/test/resources/input", "/home/vagrant/input");

            CmdLine cmd = new CmdLine();
            cmd.addRaw("java -cp /home/vagrant/test.jar " +
                    "dk.kb.hadoop.nark.CDXJob " +
                    "file:///home/vagrant/input/inputs.txt " +
                    "output");
            // cmd.addRaw("ls /home/vagrant/input");
            connection.execute(cmd); // Starts the hadoop job
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void scpFile(OverthereConnection remoteConn, String localPath, String remotePath) throws IOException {
        File localFile = new File(localPath); // Local path
        OverthereFile remoteFile = remoteConn.getFile(remotePath); // Remote path

        if (localFile.exists() && localFile.isFile()) {
            BufferedInputStream bis = new BufferedInputStream(new FileInputStream(localFile));
            BufferedOutputStream bos = new BufferedOutputStream(remoteFile.getOutputStream());

            int bytesRead = 0;
            byte[] bucket = new byte[32 * 1024];
            while (bytesRead != -1) {
                bytesRead = bis.read(bucket); //-1, 0, or more
                if (bytesRead > 0) {
                    bos.write(bucket, 0, bytesRead);
                }
            }
            bis.close();
            bos.close();
        } else { // else it's a directory
            // Throw some error
        }
    }

    private static void scpFolder(OverthereConnection remoteConn, String localPath, String remotePath) throws IOException {
        File localFolder = new File(localPath); // Local path
        OverthereFile remoteFolder = remoteConn.getFile(remotePath); // Remote path
        final File[] files = localFolder.listFiles();

        if (!remoteFolder.exists()) {
            remoteFolder.mkdir();
        }
        if (files != null) {
            for (File file : files) {
                scpFile(remoteConn, file.getPath(), FilenameUtils.concat(remoteFolder.getPath(), file.getName()));
            }
        }
    }
}
