package cs276.assignments;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.List;

public class BasicIndex implements BaseIndex {


    /**
     *
     * @param fc a FileChannel whose position is at the beginning of a posting list
     * @return a PostList
     */
    @Override
    public PostingList readPosting(FileChannel fc) {


        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES*2);
        try {
            fc.read(buffer);

        }catch (IOException e){
            e.printStackTrace();
        }

        buffer.flip();
        if(buffer.hasRemaining()) {
            int termId = buffer.getInt();
            int listSize = buffer.getInt();

            buffer.flip();
            buffer = ByteBuffer.allocate(Integer.BYTES * listSize);
            List<Integer> DocList = new LinkedList<>();

            try {
                fc.read(buffer);

            } catch (IOException e) {
                e.printStackTrace();
            }
            buffer.flip();
            for (int i = 0; i < listSize; i++)
                if (buffer.hasRemaining()) {
                    DocList.add(Integer.valueOf(buffer.getInt()));

                }


            System.out.println("Size Of DocList is : " + DocList.size());
            return new PostingList(termId, DocList);
        }

        return null;
    }

    @Override
    public void writePosting(FileChannel fc, PostingList p) {
        // a PostingList is stored on disk as:
        //     |term ID|list length|doc ID1|doc ID2|...|doc IDn|
        // all value are int32.


        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES*(2+p.getList().size()));
        buffer.putInt(p.getTermId());
        buffer.putInt(p.getList().size());
        p.getList().forEach(buffer::putInt);

        buffer.flip();
        try {
            fc.write(buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}