package surfstore;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.io.FileNotFoundException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.OutputStream ;
import java.io.FileInputStream;
import java.io.FileOutputStream ;
import com.google.protobuf.ByteString;

import java.lang.Math;

import java.util.*;
import surfstore.SurfStoreBasic.FileInfo;
import surfstore.SurfStoreBasic.Block;
import surfstore.SurfStoreBasic.WriteResult;
import surfstore.SurfStoreBasic.SimpleAnswer;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import surfstore.SurfStoreBasic.Empty;


public final class Client {
    private static final Logger logger = Logger.getLogger(Client.class.getName());

    private static final int BLOCK_SIZE = 4096;

    protected HashMap<String, byte[]> local_block;
    protected List<String> localFile;

    private final ManagedChannel metadataChannel;
    private final MetadataStoreGrpc.MetadataStoreBlockingStub metadataStub; // leader

    private final ManagedChannel metadataChannel2;
    private final MetadataStoreGrpc.MetadataStoreBlockingStub metadataStub2;
//
//    private final ManagedChannel metadataChannel3;
//    private final MetadataStoreGrpc.MetadataStoreBlockingStub metadataStub3;

    private final ManagedChannel blockChannel;
    private final BlockStoreGrpc.BlockStoreBlockingStub blockStub;

    private final ConfigReader config;

    public Client(ConfigReader config) {

//        if (config.getNumMetadataServers() > 1) {
//            Integer leader = config.getLeaderNum();
//            this.metadataChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(leader))
//                    .usePlaintext(true).build();
//            this.metadataStub = MetadataStoreGrpc.newBlockingStub(metadataChannel);
//
//            int follower1, follower2;
//            if (leader == 1) {
//                follower1 = 2;
//                follower2 = 3;
//            } else if (leader == 2) {
//                follower1 = 1;
//                follower2 = 3;
//            } else {
//                follower1 = 1;
//                follower2 = 2;
//            }
//
//            this.metadataChannel2 = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(follower1))
//                    .usePlaintext(true).build();
//            this.metadataStub2 = MetadataStoreGrpc.newBlockingStub(metadataChannel2);
//
//            this.metadataChannel3 = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(follower2))
//                    .usePlaintext(true).build();
//            this.metadataStub3 = MetadataStoreGrpc.newBlockingStub(metadataChannel3);
//        } else {
//            this.metadataChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(1))
//                    .usePlaintext(true).build();
//            this.metadataStub = MetadataStoreGrpc.newBlockingStub(metadataChannel);
//
//            this.metadataChannel2 = null;
//            this.metadataStub2 = null;
//            this.metadataChannel3 = null;
//            this.metadataStub3 = null;
//        }

        if (config.getNumMetadataServers() > 1) {
            Integer leader = config.getLeaderNum();
            this.metadataChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(leader))
                    .usePlaintext(true).build();
            this.metadataStub = MetadataStoreGrpc.newBlockingStub(metadataChannel);

            this.metadataChannel2 = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(2))
                    .usePlaintext(true).build();
            this.metadataStub2 = MetadataStoreGrpc.newBlockingStub(metadataChannel2);

        } else {
            this.metadataChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(1))
                    .usePlaintext(true).build();
            this.metadataStub = MetadataStoreGrpc.newBlockingStub(metadataChannel);

            this.metadataChannel2 = null;
            this.metadataStub2 = null;
        }

        this.blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
                .usePlaintext(true).build();
        this.blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);

        this.config = config;
    }

    public void shutdown() throws InterruptedException {
        metadataChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        blockChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    private void ensure(boolean b) {
	    if (b == false) {
	        throw new RuntimeException("Assertion failed!");
	    }
    }

    private void testBlockServer() {
        blockStub.ping(Empty.newBuilder().build());
        logger.info("Successfully pinged the Blockstore server");

        // TODO: Implement your client here
        Block b1 = stringToBlock("block_01");
        Block b2 = stringToBlock("block_02");

        ensure(blockStub.hasBlock(b1).getAnswer() == false);
        ensure(blockStub.hasBlock(b2).getAnswer() == false);

        blockStub.storeBlock(b1);
        ensure(blockStub.hasBlock(b1).getAnswer() == true);

        blockStub.storeBlock(b2);
        ensure(blockStub.hasBlock(b2).getAnswer() == true);

        Block b1prime = blockStub.getBlock(b1);
        ensure(b1prime.getHash().equals(b1.getHash()));
        ensure(b1prime.getData().equals(b1.getData()));

        logger.info("We passed all the tests...");
    }

    private void getVersion(String file_name) {
        logger.info("Calling getVerion");

        List<Integer> res = new ArrayList<>();
        FileInfo request = FileInfo.newBuilder().setFilename(file_name).build();
        FileInfo response = metadataStub.getVersion(request);
        res = response.getAllVersionsList();

        if (res.size() > 1) {
            for (Integer i : res) {
                System.out.print(i + " ");
            }
            System.out.println();
        } else {
            int version = response.getVersion();
            System.out.println(version);
        }


    }

    private void upload(String path) {
        logger.info("Calling upload");

        Map<String, Block> blockMap = new HashMap<>();
        List<String> list = new LinkedList<>();

        String[] slist = path.split("/");
        String file_name = slist[slist.length-1];

        try{
            int byteread = 0;
            byte[] buffer = new byte[BLOCK_SIZE];
            FileInputStream in = new FileInputStream(path);
            while((byteread = in.read(buffer))!=-1){
                Block block = BytesToBlock(buffer,byteread);
                String hash = block.getHash();
                list.add(hash);
                blockMap.put(hash, block);
            }
        }
        catch (IOException e){
            e.printStackTrace();
        }

        //readfile rpc
        FileInfo request = FileInfo.newBuilder().setFilename(file_name).build();
        FileInfo response = metadataStub.readFile(request);

        int version = response.getVersion();
        FileInfo req = FileInfo.newBuilder().setFilename(file_name)
                .setVersion(version + 1)
                .addAllBlocklist(list)
                .build();

        // modify rpc
        WriteResult res = metadataStub.modifyFile(req);

        int value = res.getResultValue();

        if (value == 0) { // OK
            System.out.println("OK");
            return;
        }
        else if (value == 2) { // Missing Blocks
            logger.info("get missingBlocks");
            //get missingBlocks
            List<String> missingBlocks = res.getMissingBlocksList();
            // storeBlock rpc
            for (String hash: missingBlocks) {
                blockStub.storeBlock(blockMap.get(hash));
            }
        }

        res = metadataStub.modifyFile(req);
        value = res.getResultValue();

        if (value == 0) {
            System.out.println("OK");
            return;
        }

        logger.info("Result value unvalid " + res.getResultValue());

    }

    private void download(String file_name, String destination) {
        logger.info("Calling download");
        String local_file = destination+"/"+file_name;

        FileInfo.Builder builder = FileInfo.newBuilder();
        builder.setFilename(file_name);
        FileInfo request = builder.build();
        FileInfo res = metadataStub.readFile(request);

        if(res.getVersion() == 0 || res.getBlocklistList().get(0).equals("0")){
            System.out.println("Not Found");
            return;
        }

        List<String> blockList = res.getBlocklistList();
        Map<String, Block> blockMap = new HashMap<>();

        try{
            int byteread = 0;
            byte[] buffer = new byte[BLOCK_SIZE];
            FileInputStream in = new FileInputStream(destination);
            while((byteread = in.read(buffer)) != -1){
                Block block = BytesToBlock(buffer,byteread);
                String hash = block.getHash();
                blockMap.put(hash, block);
            }
        }
        catch (IOException e){
        }

        // find missing blocks
        List<String> missingBlocks = new ArrayList<>();
        for (String block : blockList) {
            if(!blockMap.containsKey(block)){
                missingBlocks.add(block);
            }
        }

        // download missing blocks
        for (String hash : missingBlocks) {
            Block b = Block.newBuilder().setHash(hash).build();
            Block block = blockStub.getBlock(b);
            blockMap.put(hash, block);
        }

        //write file
        OutputStream out = null;
        try{
            out = new FileOutputStream(local_file);
        } catch (FileNotFoundException e)
        {}

        for(String hash : blockList){
            Block b = blockMap.get(hash);
            byte[] data = b.getData().toByteArray();
            try{
                out.write(data);
            }
            catch (IOException e)
            {}

        }
        try{
            out.close();
        }
        catch (IOException e)
        {}

        System.out.println("OK");
    }

    private void delete(String file_name) {
        logger.info("Calling delete");

        FileInfo req = FileInfo.newBuilder().setFilename(file_name).build();
        FileInfo response = metadataStub.getVersion(req);
        int version = response.getVersion();
        if(version == 0){
            System.out.println("Not Found");
            return;
        }
        FileInfo.Builder builder = FileInfo.newBuilder();
        builder.setFilename(file_name);
        builder.setVersion(version+1);
        FileInfo request = builder.build();

        WriteResult res = metadataStub.deleteFile(request);
        int value = res.getResultValue();
        if (value == 0) {
            System.out.println("OK");
            return;
        }

        logger.info("Result value unvalid " + res.getResultValue());

    }

    private void crash() {
        logger.info("crashing server number 2");
        metadataStub2.crash(Empty.newBuilder().build());
    }

    private void restore() {
        logger.info("restoring server number 2");
        metadataStub2.restore(Empty.newBuilder().build());
    }

    private void go(ConfigReader config, String operation, String file_name, String destination) {
	    metadataStub.ping(Empty.newBuilder().build());
        logger.info("Successfully pinged the Metadata server");

        blockStub.ping(Empty.newBuilder().build());
        logger.info("Successfully pinged the Blockstore server");

        /*
        * You can print out whatever messages you want to stderr. To stdout, please either print:
        * OK: if the operation succeeded
        * Not Found: if the client tried to download a file that was not found on the server, or tried to upload a file which does not exist on the local machine.
        *               Also return this if GetVersion is called on a file which does not exist.
        * */
        if (operation.equals("getversion")){
            getVersion(file_name);
        } else if (operation.equals("upload")) {
            upload(file_name);
        }
        else if (operation.equals("delete")) {
            delete(file_name);
        }
        else if (operation.equals("download")) {
            download(file_name, destination);
        }
        else if (operation.equals("crash")) {
            crash();
        } else if (operation.equals("restore")) {
            restore();
        }
        else { // unsupported operation
            System.exit(1);
        }

    }

    //helper methods

    private static Block stringToBlock(String s) {
        Block.Builder builder = Block.newBuilder();

        try {
            builder.setData(ByteString.copyFrom(s, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        builder.setHash(HashUtils.sha256(s));

        return builder.build(); //turns the Builder into a Block
    }

    private static Block BytesToBlock(byte[] buffer, int byteread){
        byte[] temp = Arrays.copyOfRange(buffer, 0, byteread);
        Block.Builder builder = Block.newBuilder();
        builder.setData(ByteString.copyFrom(temp));
        builder.setHash(HashUtils.sha256(temp));
        return builder.build();
    }

	/*
	 * TODO: Add command line handling here
	 */
    private static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("Client").build()
                .description("Client for SurfStore");
        parser.addArgument("config_file").type(String.class)
                .help("Path to configuration file");

        parser.addArgument("operation").nargs("?").type(String.class).setDefault("delete")
                .help("Operation on files");
        parser.addArgument("file_name").nargs("?").type(String.class).setDefault("/")
                .help("filename or filepath");
        parser.addArgument("destination").nargs("?").type(String.class)
                .setDefault(" ").help("destination path");


        Namespace res = null;
        try {
            res = parser.parseArgs(args);
        } catch (ArgumentParserException e){
            parser.handleError(e);
        }
        return res;
    }

    public static void main(String[] args) throws Exception {
        Namespace c_args = parseArgs(args);
        if (c_args == null){
            throw new RuntimeException("Argument parsing failed");
        }

        File configf = new File(c_args.getString("config_file"));
        ConfigReader config = new ConfigReader(configf);

        Client client = new Client(config);

        String operation = c_args.getString("operation");

        String file_name = c_args.getString("file_name");

        String destination = c_args.getString("destination");
        
        try {
        	client.go(config, operation, file_name, destination);
        } finally {
            client.shutdown();
        }
    }

}
