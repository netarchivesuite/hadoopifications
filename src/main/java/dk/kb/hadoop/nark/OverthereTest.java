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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class OverthereTest {
    final static Logger log = LoggerFactory.getLogger(OverthereTest.class);

    public static void main(String[] args) {
        ConnectionOptions options = new ConnectionOptions();
        options.set(ADDRESS, "node1");
        options.set(USERNAME, "vagrant");
        options.set(PASSWORD, "vagrant");
        options.set(OPERATING_SYSTEM, UNIX);
        options.set(CONNECTION_TYPE, SCP);

        try (OverthereConnection connection = Overthere.getConnection("ssh", options)) {
            scpFile(connection, "./target/hadoopifications-1.0-SNAPSHOT-withdeps.jar", "/home/vagrant/job.jar");
            scpFolder(connection, "./src/test/resources/input", "/home/vagrant/input");
            copyInputToHDFS(new Path("./src/test/resources/input/inputs.txt"));

            CmdLine cmd = new CmdLine();
            cmd.addRaw("java -cp /home/vagrant/job.jar " +
                    "dk.kb.hadoop.nark.CDXJob " +
                    "hdfs://node1/user/vagrant/inputs.txt " +
                    "hdfs://node1/user/vagrant/output");
            log.info("Starting CDXJob");
            connection.execute(cmd); // Starts the hadoop job
        } catch (IOException e) {
            log.error("Failed to run CDXJob");
            e.printStackTrace();
        }
    }

    /**
     * Copies a file from the local file system to a remote destination path using an OverthereConnection.
     * @param remoteConn A configured OverthereConnection specifying the remote machine that is ssh'ed into.
     * @param localPath A string path to the local file to scp.
     * @param remotePath A string path specifying the remote destination of the scp'ed file.
     * @throws IOException
     */
    private static void scpFile(OverthereConnection remoteConn, String localPath, String remotePath) throws IOException {
        File localFile = new File(localPath);
        OverthereFile remoteFile = remoteConn.getFile(remotePath);

        if (localFile.exists() && localFile.isFile()) {
            log.debug("Copying file {} to {}", localPath, remotePath);
            BufferedInputStream bis = new BufferedInputStream(new FileInputStream(localFile));
            BufferedOutputStream bos = new BufferedOutputStream(remoteFile.getOutputStream());

            int bytesRead = 0;
            byte[] bucket = new byte[32 * 1024]; // Not sure what buffer size fits best
            while (bytesRead != -1) {
                bytesRead = bis.read(bucket); //-1, 0, or more
                if (bytesRead > 0) {
                    bos.write(bucket, 0, bytesRead);
                }
            }
            bis.close();
            bos.close();
        } else {
            log.error("Failed to copy {} to {}", localPath, remotePath);
            // Throw some error
        }
    }

    /**
     * Copies a folder from the local file system to a remote destination path using an OverthereConnection.
     * @param remoteConn A configured OverthereConnection specifying the remote machine that is ssh'ed into.
     * @param localPath A string path to the local folder to scp.
     * @param remotePath A string path specifying the remote destination of the scp'ed folder.
     * @throws IOException
     */
    private static void scpFolder(OverthereConnection remoteConn, String localPath, String remotePath) throws IOException {
        File localFolder = new File(localPath);
        OverthereFile remoteFolder = remoteConn.getFile(remotePath);
        final File[] files = localFolder.listFiles();

        if (files == null) {
            // Throw error?
            log.error("No files contained in the specified folder at {}", localPath);
            return;
        }
        if (!remoteFolder.exists()) {
            log.debug("Creating missing folders in path " + localPath);
            remoteFolder.mkdirs();
        }

        for (File file : files) {
            scpFile(remoteConn, file.getPath(), FilenameUtils.concat(remoteFolder.getPath(), file.getName()));
        }
    }

    private static void copyInputToHDFS(Path inputFilePath) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://node1");
        FileSystem fs = FileSystem.get(conf);
        fs.copyFromLocalFile(inputFilePath, new Path("/user/vagrant/inputs.txt"));
    }
}
