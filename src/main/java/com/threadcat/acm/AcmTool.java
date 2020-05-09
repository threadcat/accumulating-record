/*
 * Copyright 2020 threadcat
 *
 *     https://github.com/threadcat/accumulating-record
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.threadcat.acm;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * Migration utility to transform ACM files changing column list.
 *
 * @author threadcat
 */
public class AcmTool {

    public static void main(String[] args) throws Exception {
        if (args.length != 1 && args.length != 3) {
            System.out.println("Usage: <source_file> <target_file> <columns>\n" +
                    "Example to skip one column and insert another one: 0-3,6,N,5");
            return;
        }
        String sourceFileName = args[0];
        ByteBuffer bufferA = openReadOnly(sourceFileName);
        switch (args.length) {
            case 1:
                printInfo(bufferA);
                return;
            case 3:
                String migrationMap = args[2];
                List<Integer> newColumns = parseMapping(migrationMap);
                // Creating new file
                String targetFileName = args[1];
                FileChannel channelB = FileChannel.open(Path.of(targetFileName),
                        StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
                // Setting file size
                int oldRows = bufferA.getInt(AcmTable.ROWS);
                int newSize = AcmTable.size(oldRows, newColumns.size());
                channelB.position(newSize);
                // Moving data
                migrate(bufferA, channelB, newColumns);
                System.out.println("Migration completed");
                break;
        }
    }

    private static void migrate(ByteBuffer bufferA, FileChannel channelB, List<Integer> columns) {
        if (startsWith(bufferA, AcmRecord.MARKER)) {
            migrateRecord(bufferA, channelB, columns);
        } else if (startsWith(bufferA, AcmTable.MARKER)) {
            migrateTable(bufferA, channelB, columns);
        } else {
            System.out.println("Unsupported file type");
        }
    }

    private static void migrateRecord(ByteBuffer bufferA, FileChannel channelB, List<Integer> columns) {
        int baseOffset = AcmRecord.DATA;
    }

    private static void migrateTable(ByteBuffer bufferA, FileChannel channelB, List<Integer> columns) {
        int baseOffset = AcmTable.DATA;
    }

    private static List<Integer> parseMapping(String desc) {
        List<Integer> result = new ArrayList<>();
        for (String s : desc.split(",")) {
            String[] aa = s.split("-");
            if (aa.length > 1) {
                int start = Integer.parseInt(aa[0]);
                int end = Integer.parseInt(aa[1]);
                for (int i = start; i < end; i++) {
                    result.add(i);
                }
            } else {
                String b = aa[0];
                if (b.equalsIgnoreCase("N")) {
                    result.add(-1);
                } else {
                    result.add(Integer.parseInt(b));
                }
            }
        }
        return result;
    }

    private static void printInfo(ByteBuffer buffer) {
        if (startsWith(buffer, AcmRecord.MARKER)) {
            int columns = recordColumns(buffer);
            System.out.printf("%s %d columns", new String(AcmRecord.MARKER), columns);
        } else if (startsWith(buffer, AcmTable.MARKER)) {
            int rows = AcmTable.rows(buffer);
            int columns = tableColumns(buffer, rows);
            System.out.printf("%s %d rows, %d columns", new String(AcmTable.MARKER), rows, columns);
        } else {
            System.out.println("Unsupported file type");
        }
    }

    private static int tableColumns(ByteBuffer buffer, int rows) {
        return (((buffer.capacity() - AcmRecord.DATA) / rows) - 16) / 16;
    }

    private static int recordColumns(ByteBuffer buffer) {
        return (buffer.capacity() - AcmRecord.DATA) / 16;
    }

    private static ByteBuffer openReadOnly(String fileName) throws IOException {
        FileChannel channel = FileChannel.open(Path.of(fileName), StandardOpenOption.READ);
        long size = channel.size();
        if (size >= Integer.MAX_VALUE) {
            throw new IllegalArgumentException("File is too large " + fileName);
        }
        ByteBuffer buffer = ByteBuffer.allocate((int) size);
        for (int n = 0; n < size && n != -1; ) {
            n += channel.read(buffer, 0);
        }
        return buffer;
    }

    static boolean startsWith(ByteBuffer buffer, byte[] marker) {
        for (int i = 0; i < marker.length; i++) {
            if (marker[i] != buffer.get(i)) {
                return false;
            }
        }
        return true;
    }
}
