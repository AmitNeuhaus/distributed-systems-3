import java.util.ArrayList;
import java.util.Arrays;
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
                maxHeadIndexedNoun = index;
            }
            index = index + 1;
        }
        if (maxHeadIndexedNoun == -1) {
            System.out.println("no nouns in ngram");
            return null;
        } else {
            return findPatternAndNouns(maxHeadIndexedNoun, records);
        }
    }

    public static PatternNoun findPatternAndNouns(Integer startIndex, ArrayList<NGramRecord> records) {
        // Getting pattern and nouns from corpus.
        NGramRecord currentNgram = records.get(startIndex);
        StringBuilder pattern = new StringBuilder();
        String noun1 = currentNgram.word;
        String noun2 = null;

        while ( currentNgram.headIndex != 0) {
            currentNgram = records.get(currentNgram.headIndex - 1);
            if (!currentNgram.tag.equals("NN")) {
                pattern.append(" ").append(currentNgram.word);
            } else {
                noun2 = currentNgram.word;
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


    //getting pattern and noun from output reducer 1.
    public static PatternNoun getPatternNoun(String value){
        List<String> nouns_pattern_list = Arrays.stream(value.split(" ")).toList();
        String noun1 = nouns_pattern_list.get(0);
        String noun2 = nouns_pattern_list.get(1);
        String pattern = "";
        for (int i=2; i< nouns_pattern_list.size(); i++){
            pattern = pattern + nouns_pattern_list.get(i);
        }
        return new PatternNoun(noun1,noun2,pattern);
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