package hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author erenwu
 */
public class HdfsApi {

    public static final String HOST = "hdfs://192.168.248.130:8020";

    public static void main(String[] args) throws Exception {
        HdfsApi hdfsApi = new HdfsApi();
        hdfsApi.mergeDownload();
    }

    /**
     * 方式一：set方式+通过get
     * @throws IOException
     */
    public void getFileSystem1() throws IOException {
        //1:创建Configuration对象
        Configuration conf = new Configuration();
        //2:设置文件系统类型
        conf.set("fs.defaultFS", HOST);
        //3:获取指定文件系统
        FileSystem fileSystem = FileSystem.get(conf);
        //4:输出测试
        System.out.println(fileSystem);
    }

    /**
     * 文件遍历。
     */
    public void listFiles() throws IOException, URISyntaxException {
        //1.获取文件实例
        FileSystem fileSystem = FileSystem.get(new URI(HOST), new Configuration());
        //2、调用方法listFiles 获取 /目录下所有的文件信息
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new
                Path("/"), true);
        //3、遍历迭代器
        while (iterator.hasNext()){
            LocatedFileStatus fileStatus = iterator.next();
            //获取文件的绝对路径 : hdfs://hadoop0:8020/xxx
            System.out.println(fileStatus.getPath() + "======"
                    +fileStatus.getPath().getName());
            //文件的block信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println("主机为: "+host);
                }
            }
            System.out.println("block数量为: "+blockLocations.length);
        }
    }

    /**
     * 创建文件夹
     */
    public void mkdir() throws Exception {
        //1、获取FileSystem
        FileSystem fileSystem = FileSystem.get(new URI(HOST), new Configuration(), "root");

        //2、创建文件夹
        fileSystem.mkdirs(new Path("/test/mkdirs"));

        //3、关闭FileSystem
        fileSystem.close();
    }

    /**
     * 上传文件
     * @throws Exception
     */
    public void upLoad() throws Exception {
        //1.获取FileSystem
        FileSystem system = FileSystem.get(new URI(HOST), new Configuration(), "root");

        //2.上传文件
        system.copyFromLocalFile(new Path("D:\\Java\\practice\\test.txt"), new Path("/test/mkdirs"));

        //3.关闭FileSystem
        system.close();
    }

    /**
     * 合并上传小文件
     */
    public void mergeUpload() throws Exception {
        //1.获取FileSystem
        FileSystem fs = FileSystem.get(new URI(HOST), new Configuration(), "root");
        //2:获取hdfs大文件的输出流
        FSDataOutputStream fsDataOutputStream = fs.create(new Path("/test/merge.txt"));
        //3:获取一个本地文件系统
        LocalFileSystem local = FileSystem.getLocal(new Configuration());
        //4:获取本地文件夹下所有文件的详情
        FileStatus[] fileStatuses = local.listStatus(new Path("D:\\Java\\practice"));
        //5:遍历每个文件，获取每个文件的输入流
        for (FileStatus fileStatus : fileStatuses) {
            FSDataInputStream inputStream = local.open(fileStatus.getPath());
            //6:将小文件的数据复制到大文件
            IOUtils.copy(inputStream, fsDataOutputStream);
            IOUtils.closeQuietly(inputStream);
        }
        //7:关闭流
        IOUtils.closeQuietly(fsDataOutputStream);
        local.close();
        fs.close();
    }

    /**
     * 合并下载小文件
     */
    public void mergeDownload() throws Exception {
        //1.获取服务器文件系统
        FileSystem fs = FileSystem.get(new URI(HOST), new Configuration(), "root");
        //2.获取服务器文件的输入流
        FileStatus[] status = fs.listStatus(new Path("/test/mkdirs"));
        //3.获取本地的输出流
        FileOutputStream outputStream = new FileOutputStream("D:\\Java\\practice\\mergeDown.txt");

        for (FileStatus fileStatus : status) {
            FSDataInputStream inputStream = fs.open(fileStatus.getPath());

            //4:将小文件的数据复制到大文件
            IOUtils.copy(inputStream, outputStream);
            IOUtils.closeQuietly(inputStream);
        }
        //5、关闭流
        IOUtils.closeQuietly(outputStream);
        fs.close();
    }
}
