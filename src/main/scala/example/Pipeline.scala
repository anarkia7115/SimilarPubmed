package example;

import org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;
import org.apache.uima.fit.factory.CollectionReaderFactory.createReaderDescription;
import org.apache.uima.fit.pipeline.SimplePipeline.runPipeline;
import de.tudarmstadt.ukp.dkpro.core.io.conll.Conll2006Writer;
import de.tudarmstadt.ukp.dkpro.core.io.text.TextReader;
import de.tudarmstadt.ukp.dkpro.core.languagetool.LanguageToolLemmatizer;
import de.tudarmstadt.ukp.dkpro.core.maltparser.MaltParser;
import de.tudarmstadt.ukp.dkpro.core.opennlp.OpenNlpPosTagger;
import de.tudarmstadt.ukp.dkpro.core.opennlp.OpenNlpSegmenter;

class Pipeline {
    def main(args:List[String] ) {
    runPipeline(
        createReaderDescription(TextReader.class,
            TextReader.PARAM_SOURCE_LOCATION, "document.txt",
            TextReader.PARAM_LANGUAGE, "en"),
        createEngineDescription(OpenNlpSegmenter.class),
        createEngineDescription(OpenNlpPosTagger.class),
        createEngineDescription(LanguageToolLemmatizer.class),
        createEngineDescription(MaltParser.class),
        createEngineDescription(Conll2006Writer.class,
            Conll2006Writer.PARAM_TARGET_LOCATION, "."));
  }
}
