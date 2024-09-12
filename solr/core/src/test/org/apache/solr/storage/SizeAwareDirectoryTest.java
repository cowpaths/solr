package org.apache.solr.storage;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.DirectoryFactory;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Random;

public class SizeAwareDirectoryTest extends SolrTestCaseJ4 {

    public void testInitSize() throws Exception {
        final String path = createTempDir().toString() + "/somedir";
        CompressingDirectoryFactory dirFac = new CompressingDirectoryFactory();;
        try (dirFac) {
            dirFac.initCoreContainer(null);
            dirFac.init(new NamedList<>());

            Directory dir =
                    dirFac.get(path, DirectoryFactory.DirContext.DEFAULT, DirectoryFactory.LOCK_TYPE_SINGLE);
            try {
                try (IndexOutput file = dir.createOutput("test_file", IOContext.DEFAULT)) {
                    file.writeInt(42);
                } // implicitly close file

                try (IndexOutput file = dir.createOutput("test_file2", IOContext.DEFAULT)) {
                    file.writeInt(84);
                } // implicitly close file

                dir.sync(Collections.singleton("test_file"));
                assertTrue(path + " should exist once file is synced", dirFac.exists(path));
                dir.sync(Collections.singleton("test_file2"));
                assertTrue(path + " should exist once file is synced", dirFac.exists(path));

                long expectedDiskSize = Files.size(Paths.get(path+"/test_file")) + Files.size(Paths.get(path+"/test_file2"));
                assertEquals("directory size should be equal to on disk size of test files", expectedDiskSize, dirFac.onDiskSize(dir));
            } finally {
                dirFac.release(dir);
            }
        }
    }

    public void testSizeTracking() throws Exception {
        // after onDiskSize has been init(), the size should be correct using the LongAdder sum in SizeAwareDirectory
        final String path = createTempDir().toString() + "/somedir";
        CompressingDirectoryFactory dirFac = new CompressingDirectoryFactory();;
        try (dirFac) {
            dirFac.initCoreContainer(null);
            dirFac.init(new NamedList<>());

            Directory dir =
                    dirFac.get(path, DirectoryFactory.DirContext.DEFAULT, DirectoryFactory.LOCK_TYPE_SINGLE);
            try {
                try (IndexOutput file = dir.createOutput("test_file", IOContext.DEFAULT)) {
                    file.writeInt(42);
                } // implicitly close file

                long expectedDiskSize = Files.size(Paths.get(path+"/test_file"));
                assertEquals("directory size should be equal to on disk size of test files", expectedDiskSize, dirFac.onDiskSize(dir));

                try (IndexOutput file = dir.createOutput("test_file2", IOContext.DEFAULT)) {
                    file.writeInt(84);
                } // implicitly close file

                expectedDiskSize =  Files.size(Paths.get(path+"/test_file")) + Files.size(Paths.get(path+"/test_file2"));
                assertEquals("directory size should be equal to on disk size of test files", expectedDiskSize, dirFac.onDiskSize(dir));
            } finally {
                dirFac.release(dir);
            }
        }
    }

    public void testWriteBigFile() throws Exception {
        final String path = createTempDir().toString() + "/somedir";
        CompressingDirectoryFactory dirFac = new CompressingDirectoryFactory();;
        try (dirFac) {
            dirFac.initCoreContainer(null);
            dirFac.init(new NamedList<>());

            Directory dir =
                    dirFac.get(path, DirectoryFactory.DirContext.DEFAULT, DirectoryFactory.LOCK_TYPE_SINGLE);
            try {
                // small file first to initSize()
                try (IndexOutput file = dir.createOutput("test_file", IOContext.DEFAULT)) {
                    file.writeInt(42);
                } // implicitly close file

                long expectedDiskSize = Files.size(Paths.get(path+"/test_file"));
                assertEquals("directory size should be equal to on disk size of test files", expectedDiskSize, dirFac.onDiskSize(dir));

                writeBlockSizeFile(dir, "test_file2");

                expectedDiskSize =  Files.size(Paths.get(path+"/test_file")) + Files.size(Paths.get(path+"/test_file2"));
                assertEquals("directory size should be equal to on disk size of test files", expectedDiskSize, dirFac.onDiskSize(dir));
            } finally {
                dirFac.release(dir);
            }
        }
    }

    public void testDelete() throws Exception {
        // write a file, then another, then delete one of the files - the onDiskSize should update correctly
        final String path = createTempDir().toString() + "/somedir";
        CompressingDirectoryFactory dirFac = new CompressingDirectoryFactory();;
        try (dirFac) {
            dirFac.initCoreContainer(null);
            dirFac.init(new NamedList<>());

            Directory dir =
                    dirFac.get(path, DirectoryFactory.DirContext.DEFAULT, DirectoryFactory.LOCK_TYPE_SINGLE);
            try {
                // small file first to initSize()
                try (IndexOutput file = dir.createOutput("test_file", IOContext.DEFAULT)) {
                    file.writeInt(42);
                } // implicitly close file

                long expectedDiskSize = Files.size(Paths.get(path+"/test_file"));
                assertEquals("directory size should be equal to on disk size of test files", expectedDiskSize, dirFac.onDiskSize(dir));

                writeBlockSizeFile(dir, "test_file2");

                expectedDiskSize =  Files.size(Paths.get(path+"/test_file")) + Files.size(Paths.get(path+"/test_file2"));
                assertEquals("directory size should be equal to on disk size of test files", expectedDiskSize, dirFac.onDiskSize(dir));

                dir.deleteFile("test_file2");
                expectedDiskSize =  Files.size(Paths.get(path+"/test_file"));
                assertEquals("directory size should be equal to on disk size of test files", expectedDiskSize, dirFac.onDiskSize(dir));
            } finally {
                dirFac.release(dir);
            }
        }
    }

    private void writeBlockSizeFile(Directory dir, String name) throws Exception {
        try (IndexOutput file = dir.createOutput(name, IOContext.DEFAULT)) {
            // write some small things first to force past blocksize boundary
            file.writeInt(42);
            file.writeInt(84);
            // write a giant blocksize thing to force compression with dump()
            Random random = new Random(42);
            int blocksize = CompressingDirectory.COMPRESSION_BLOCK_SIZE;
            byte[] byteArray = new byte[blocksize];
            random.nextBytes(byteArray);
            file.writeBytes(byteArray, blocksize);
        } // implicitly close file
    }

    private void writeRandomFileOfSize(Directory dir, String name, int size) throws Exception {
        try (IndexOutput file = dir.createOutput(name, IOContext.DEFAULT)) {
            // write a giant blocksize thing to force compression with dump()
            Random random = new Random(42);
            byte[] byteArray = new byte[size];
            random.nextBytes(byteArray);
            file.writeBytes(byteArray, size);
        } // implicitly close file
    }
}
