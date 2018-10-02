package surfstore;

import java.io.File;
import java.util.*;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import surfstore.SurfStoreBasic.*;
import com.google.protobuf.ByteString;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;


public final class MetadataStore {
    private static final Logger logger = Logger.getLogger(MetadataStore.class.getName());

    protected Server server;
	protected ConfigReader config;

    // connect with blockStore
    private ManagedChannel blockChannel;
    private BlockStoreGrpc.BlockStoreBlockingStub blockStub;

    // connect with following metadataStore
    private ManagedChannel metadataChannel;
    private MetadataStoreGrpc.MetadataStoreBlockingStub metadataStub;

    public MetadataStore(ConfigReader config) {
    	this.config = config;
	}



	private void start(int serverId, int port, int numThreads) throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(new MetadataStoreImpl(config, serverId))
                .executor(Executors.newFixedThreadPool(numThreads))
                .build()
                .start();
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                MetadataStore.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    private static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("MetadataStore").build()
                .description("MetadataStore server for SurfStore");
        parser.addArgument("config_file").type(String.class)
                .help("Path to configuration file");
        parser.addArgument("-n", "--number").type(Integer.class).setDefault(1)
                .help("Set which number this server is");
        parser.addArgument("-t", "--threads").type(Integer.class).setDefault(10)
                .help("Maximum number of concurrent threads");

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

        if (c_args.getInt("number") > config.getNumMetadataServers()) {
            throw new RuntimeException(String.format("metadata%d not in config file", c_args.getInt("number")));
        }

        final MetadataStore server = new MetadataStore(config);
        server.start(c_args.getInt("number"), config.getMetadataPort(c_args.getInt("number")), c_args.getInt("threads"));
        server.blockUntilShutdown();
    }

    class MetadataStoreImpl extends MetadataStoreGrpc.MetadataStoreImplBase {

        // map to store filename - fileinfo pair
        protected Map<String, surfstore.SurfStoreBasic.FileInfo> metaMap;
        private int committedLength;
        private List<Log> logList;
        protected boolean isLeader;
        protected boolean isCrashed;
        protected boolean isDistributed;
        protected int serverId;

        protected ArrayList<MetadataStoreGrpc.MetadataStoreBlockingStub> followers;


        public MetadataStoreImpl(ConfigReader config, int serverId) {
            super();
            this.metaMap = new HashMap<String, surfstore.SurfStoreBasic.FileInfo>();
            logList = new ArrayList<>();
            this.isCrashed = false;
            Timer timer = new Timer();
            committedLength = 0;
            this.serverId = serverId;

            if (config.getNumMetadataServers() > 1) {
                isDistributed = true;
            }

            if (config.getLeaderNum() == serverId) {
                isLeader = true;
            }

            if (isLeader) {

                blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
                        .usePlaintext(true).build();
                blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);

                followers = new ArrayList<MetadataStoreGrpc.MetadataStoreBlockingStub>();

                // add all followers
                for(int i = 1; i <= config.getNumMetadataServers(); i++)
                {
                    if (i == serverId) {
                        continue;
                    }
                    ManagedChannel mc = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(i)).usePlaintext(true).build();
                    MetadataStoreGrpc.MetadataStoreBlockingStub metadataStub = MetadataStoreGrpc.newBlockingStub(mc);

                    followers.add(metadataStub);
                }

                if(isDistributed)
                {
                    timer.schedule(new TimerTask() {
                        @Override
                        public void run() {
                            try {

                                for(MetadataStoreGrpc.MetadataStoreBlockingStub stub : followers)
                                {
                                    updateLog(stub);
                                }
                            }
                            catch(Exception E) {}	//this will catch if followers aren't running yet
                        }
                    }, 0, 500);
                }
            }
        }

        @Override
        public void ping(surfstore.SurfStoreBasic.Empty request, final StreamObserver<Empty> responseObserver) {
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        // TODO: Implement the other RPCs!

        @Override
        public void readFile(surfstore.SurfStoreBasic.FileInfo request,
                             io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.FileInfo> responseObserver) {

            String filename = request.getFilename();
            FileInfo.Builder builder = FileInfo.newBuilder();
            builder.setFilename(filename);


            int version = 0;
            // get version
            if (metaMap.containsKey(filename)) {
                version = metaMap.get(filename).getVersion();
                if (version < 0) version = 0;
            }

            if(!isCrashed && version != 0)
            {
                builder.addAllBlocklist(metaMap.get(filename).getBlocklistList());
            }
            builder.setVersion(version);
            FileInfo response = builder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();

        }
//        message WriteResult {
//            enum Result {
//                OK = 0;
//                OLD_VERSION = 1;
//                MISSING_BLOCKS = 2;
//                NOT_LEADER = 3;
//            }
//            Result result = 1;
//            int32 current_version = 2;
//            repeated string missing_blocks = 3;
//        }

        @Override
        public void modifyFile(surfstore.SurfStoreBasic.FileInfo request,
                               io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.WriteResult> responseObserver) {

            synchronized (this) {
                WriteResult.Builder resultBuilder = WriteResult.newBuilder();
                Block.Builder blockBuilder = Block.newBuilder();
                boolean missing = false;
                String filename = request.getFilename();
                int client_version = request.getVersion();
                int server_version = metaMap.containsKey(filename) ? metaMap.get(filename).getVersion() : 0;

                List<String> missBlockList = new ArrayList<String>();

                // check missing blocks
                for (String s : request.getBlocklistList()) {
                    blockBuilder.setHash(s);
                    Block block = blockBuilder.build();
                    if (blockStub.hasBlock(block).getAnswer() == false) {
                        missBlockList.add(s);
                        missing = true;
                    }
                }

                if(!isLeader)
                {
                    logger.info("calling modify on non-leader server");
                    resultBuilder.setResult(WriteResult.Result.NOT_LEADER);
                    return;
                }
                else if (server_version != client_version - 1) {
                    resultBuilder.setCurrentVersion(server_version);
                    resultBuilder.setResult(WriteResult.Result.OLD_VERSION);
                }
                else if (missing) {
                    logger.info("missing blocks");
                    resultBuilder.setResult(WriteResult.Result.MISSING_BLOCKS);
                    resultBuilder.setCurrentVersion(server_version);
                    resultBuilder.addAllMissingBlocks(missBlockList);
                }
                else {
                    FileInfo.Builder infoBuilder = FileInfo.newBuilder();
                    infoBuilder.setVersion(client_version);
                    infoBuilder.setFilename(filename);
                    infoBuilder.addAllBlocklist(request.getBlocklistList());
                    FileInfo info = infoBuilder.build();
                    Log entry = LogConstructor(committedLength, info);

                    if (twoPhase(entry)) {
                        resultBuilder.setCurrentVersion(client_version);
                        resultBuilder.setResult(WriteResult.Result.OK);
                        // update
                        metaMap.put(filename, info);
                        logger.info("file updated");
                    } else {
                        // not enough votes
                        logger.info("Not enough votes.");
                    }
                }

                WriteResult response = resultBuilder.build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
        }

        @Override
        public void deleteFile(surfstore.SurfStoreBasic.FileInfo request,
                               io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.WriteResult> responseObserver) {
            synchronized (this) {
                WriteResult.Builder resultBuilder = WriteResult.newBuilder();
                Block.Builder builder = Block.newBuilder();
                String filename = request.getFilename();
                int client_version = request.getVersion();
                int server_version = metaMap.containsKey(filename) ? metaMap.get(filename).getVersion() : 0;

                if (!isLeader) {
                    logger.info("calling delete on non-leader server");
                    resultBuilder.setResult(WriteResult.Result.NOT_LEADER);
                } else if (server_version != client_version - 1) {
                    logger.info("1: OLD_VERSION");
                    resultBuilder.setCurrentVersion(server_version);
                    resultBuilder.setResult(WriteResult.Result.OLD_VERSION);
                } else {
                    FileInfo.Builder infoBuilder = FileInfo.newBuilder();
                    infoBuilder.setVersion(client_version);
                    infoBuilder.setFilename(filename);
                    infoBuilder.addAllBlocklist(Arrays.asList("0"));
                    FileInfo info = infoBuilder.build();
                    Log entry = LogConstructor(committedLength, info);

                    if (twoPhase(entry)) {
                        resultBuilder.setCurrentVersion(client_version);
                        resultBuilder.setResult(WriteResult.Result.OK);
                        // update
                        metaMap.put(filename, info);
                        logger.info("file deleted");
                    } else {
                        // not enough votes
                        logger.info("Not enough votes.");
                    }
                }
                WriteResult result = resultBuilder.build();
                responseObserver.onNext(result);
                responseObserver.onCompleted();
            }
        }

        @Override
        public void getVersion(surfstore.SurfStoreBasic.FileInfo request,
                               io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.FileInfo> responseObserver) {
            logger.info("client try to get file version");

            String filename = request.getFilename();

            FileInfo.Builder fileBuilder = FileInfo.newBuilder();

            if (isLeader) {
                int leader_version = metaMap.containsKey(filename) ? metaMap.get(filename).getVersion() : 0;
                List<Integer> versions = new ArrayList<>();
                versions.add(leader_version);
                // get follower versions
                if (followers.size() > 0) {
                    for (MetadataStoreGrpc.MetadataStoreBlockingStub follow : followers) {
                        FileInfo f = follow.getVersion(request);
                        versions.add(f.getVersion());
                    }
                }
                // set allversions
                for (Integer val : versions) {
                    fileBuilder.addAllVersions(val);
                }
                fileBuilder.setFilename(filename).setVersion(leader_version);
            }

            // follower
            else {
                int version = metaMap.containsKey(filename) ? metaMap.get(filename).getVersion() : 0;
                fileBuilder.setFilename(filename).setVersion(version);
            }

            //response
            FileInfo response = fileBuilder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();

        }

        @Override
        public void isLeader(surfstore.SurfStoreBasic.Empty request,
                             io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.SimpleAnswer> responseObserver) {

            SimpleAnswer.Builder builder = SimpleAnswer.newBuilder();
            builder.setAnswer(isLeader);
            SimpleAnswer response = builder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void crash(surfstore.SurfStoreBasic.Empty request,
                          io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.Empty> responseObserver) {

            isCrashed = true;
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void restore(surfstore.SurfStoreBasic.Empty request,
                            io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.Empty> responseObserver) {

            isCrashed = false;
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void isCrashed(surfstore.SurfStoreBasic.Empty request,
                              io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.SimpleAnswer> responseObserver) {

            SimpleAnswer response = SimpleAnswer.newBuilder().setAnswer(isCrashed).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        // two phase commit
        public void updateLog(MetadataStoreGrpc.MetadataStoreBlockingStub metadataStub){
            int localLength = metadataStub.getLogLength(Empty.newBuilder().build()).getLength();

            // check if need to update follower's log
            if(committedLength == localLength) return;

            for(int i = localLength; i < committedLength; i++){

                // rpc on followers to append missing log entry,
                if(!metadataStub.appendEntries(logList.get(i)).getAnswer()) {
                    // break if follower is crashed or gap exist between appending log and local log
                    break;
                }
                // execute the log if succussfully append missing entry
                metadataStub.executeLog(logList.get(i));
            }
        }

        public boolean twoPhase(Log log) {
            // first append to its own log
            logList.add(log);
            // vote
            int count = 0;
            for (MetadataStoreGrpc.MetadataStoreBlockingStub follow : followers) {
                if (follow.appendEntries(log).getAnswer()) {
                    count++;
                }
            }

            if (count >= followers.size() / 2) {
                // commit
                committedLength++; // record index in leader

                for (MetadataStoreGrpc.MetadataStoreBlockingStub follow : followers) {
                    if (follow.appendEntries(log).getAnswer()){
                        follow.executeLog(log); // follower commit
                    }

                }
                return true;
            } else {
                // did not get enough votes
                logList.remove(logList.size() - 1);
                for (MetadataStoreGrpc.MetadataStoreBlockingStub follow : followers) {
                    follow.abort(log);
                }
                return false;
            }
        }

        @Override
        public void abort(Log log, final StreamObserver<Empty> responseObserver) {

            if (logList.get(logList.size() - 1).getIndex() == log.getIndex() && !isCrashed) {
                logList.remove(logList.size() - 1);
            }

            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void getLogLength(surfstore.SurfStoreBasic.Empty request,
                                 io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.LogLength> responseObserver){

            LogLength response = LogLength.newBuilder().setLength(committedLength).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();

        }

        @Override
        public void appendEntries(surfstore.SurfStoreBasic.Log request,
                                  io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.SimpleAnswer> responseObserver){

            SimpleAnswer.Builder answerBuilder = SimpleAnswer.newBuilder();

            // if crashed
            if (isCrashed) {
                answerBuilder.setAnswer(false);
            }
            else if (committedLength == request.getIndex()) {
                answerBuilder.setAnswer(true);
                logList.add(request);
            } else {
                // gap between commit index
                answerBuilder.setAnswer(false);
            }
            SimpleAnswer answer = answerBuilder.build();
            responseObserver.onNext(answer);
            responseObserver.onCompleted();

        }

        @Override
        public void executeLog(surfstore.SurfStoreBasic.Log request,
                               io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.Empty> responseObserver){

            // allow log to be executed
            if (!isCrashed && committedLength == request.getIndex()) {
                committedLength++;
                FileInfo newInfo = request.getFile();
                metaMap.put(newInfo.getFilename(), newInfo);
            }

            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        private Log LogConstructor(int index, FileInfo file) {
            Log log = Log.newBuilder().setIndex(index).setFile(file).build();
            return log;
        }

    }
}