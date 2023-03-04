import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.io.IOException;
import java.io.StringReader;
import java.util.*;

public class Parser {
    public static PatternNoun parse(String sentence) throws IOException {
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
            return null;
        } else {
            return findPatternAndNouns(maxHeadIndexedNoun, records);
        }
    }

    public static PatternNoun findPatternAndNouns(Integer startIndex, ArrayList<NGramRecord> records) throws IOException {
        // Getting pattern and nouns from corpus.
        NGramRecord currentNgram = records.get(startIndex);
        StringJoiner pattern = new StringJoiner(" ");
        String noun1 = currentNgram.word;
        String noun2 = null;

        while ( currentNgram.headIndex != 0) {
            currentNgram = records.get(currentNgram.headIndex - 1);
            if (!currentNgram.tag.equals("NN")) {
                pattern.add(currentNgram.word);
            } else {
                noun2 = currentNgram.word;
                break;
            }
        }
        if (noun2 == null) {
            return null;
        } else {
            System.out.println("Found correct pattern-noun");
            return new PatternNoun(noun1, noun2, pattern.toString(), false);
        }
    }



    public static String stem(String text) throws IOException {
        // Create a token stream from the input text
        TokenStream tokenStream = new EnglishAnalyzer().tokenStream("", new StringReader(text));
        CharTermAttribute charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);

        StringJoiner stemmedSentence = new StringJoiner(" ");
        // Reset the token stream and iterate over its tokens
        tokenStream.reset();
        while (tokenStream.incrementToken()) {
            stemmedSentence.add(charTermAttribute.toString());
        }

        // Close the token stream
        tokenStream.close();
        return stemmedSentence.toString();
    }


    //getting pattern and noun from output reducer 1.
    public static PatternNoun getPatternNoun(String value) throws IOException {
        List<String> nouns_pattern_list = Arrays.asList(value.split(" "));
        String noun1 = nouns_pattern_list.get(0);
        String noun2 = nouns_pattern_list.get(1);
        String pattern = "";
        for (int i=2; i< nouns_pattern_list.size(); i++){
            pattern = pattern + " " + nouns_pattern_list.get(i);
        }
        return new PatternNoun(noun1,noun2,pattern, false);
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