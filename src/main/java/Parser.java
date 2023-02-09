import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Parser {
    public static PatternNoun parse(String sentence) {
        String[] seperatedNgrams = sentence.split(" ");

        ArrayList<NGramRecord> records = new ArrayList<>();
        int maxHeadIndexedNoun = -1;
        int index = 0;
        for (String nGram : seperatedNgrams) {
            NGramRecord ngramRecord = new NGramRecord(nGram);
            records.add(ngramRecord);
            if (ngramRecord.headIndex > maxHeadIndexedNoun && ngramRecord.tag.equals("NN")) {
                maxHeadIndexedNoun = ngramRecord.headIndex;
            }
            index = index + 1;
        }
        if (maxHeadIndexedNoun == -1) {
            throw new IllegalArgumentException("no nouns in ngram");
        } else {
            return findPatternAndNouns(maxHeadIndexedNoun, records);
        }
    }

    public static PatternNoun findPatternAndNouns(Integer startIndex, ArrayList<NGramRecord> records) {
        NGramRecord currentNgram = records.get(startIndex);
        StringBuilder pattern = new StringBuilder();
        String noun1 = currentNgram.word;
        String noun2 = null;

        int searchLimit = 3; // limited by amount of words in corpus sentence.
        for (int i = 0; i < searchLimit; i++) {
            NGramRecord next = records.get(currentNgram.headIndex);
            if (!next.tag.equals("NN")) {
                pattern.append(" ").append(next.word);
            } else {
                noun2 = next.word;
                break;
            }
        }
        if (noun2 == null) {
            System.out.println("No second noun found");
            return null;
        } else {
            return new PatternNoun(noun1, noun2, String.valueOf(pattern));
        }
    }

    static class NGramRecord {
        public String word;
        public String tag;
        public String depLabel;
        public int headIndex;

        public NGramRecord(String nGram) {
            String[] partsInNgram = nGram.split("/");
            this.word = partsInNgram[0];
            this.tag = partsInNgram[1];
            this.depLabel = partsInNgram[2];
            this.headIndex = (Integer.parseInt(partsInNgram[3]));
        }

    }
}