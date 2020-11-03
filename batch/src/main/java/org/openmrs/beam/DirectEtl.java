/*
 * This Source Code Form is subject to the terms of the Mozilla Public License,
 * v. 2.0. If a copy of the MPL was not distributed with this file, You can
 * obtain one at http://mozilla.org/MPL/2.0/. OpenMRS is also distributed under
 * the terms of the Healthcare Disclaimer located at http://openmrs.org/license.
 *
 * Copyright (C) OpenMRS Inc. OpenMRS is a registered trademark and the OpenMRS
 * graphic logo is a trademark of OpenMRS Inc.
 */
package org.openmrs.beam;

import java.sql.ResultSet;
import java.util.HashSet;
import java.util.Iterator;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.openmrs.BaseOpenmrsData;
import org.openmrs.OpenmrsData;
import org.openmrs.Person;
import org.openmrs.PersonName;

/**
 * An example that counts words in Shakespeare and includes Beam best practices.
 *
 * <p>This class, {@link WordCount}, is the second in a series of four successively more detailed
 * 'word count' examples. You may first want to take a look at {@link MinimalWordCount}. After
 * you've looked at this example, then see the {@link DebuggingWordCount} pipeline.
 *
 * <p>Basic concepts, also in the MinimalWordCount example: Reading text files; counting a
 * PCollection; writing to text files
 *
 * <p>New Concepts:
 *
 * <pre>
 *   1. Executing a Pipeline both locally and using the selected runner
 *   2. Using ParDo with static DoFns defined out-of-line
 *   3. Building a composite transform
 *   4. Defining your own pipeline options
 * </pre>
 *
 * <p>Concept #1: you can execute this pipeline either locally or using by selecting another runner.
 * These are now command-line options and not hard-coded as they were in the MinimalWordCount
 * example.
 *
 * <p>To change the runner, specify:
 *
 * <pre>{@code
 * --runner=YOUR_SELECTED_RUNNER
 * }</pre>
 *
 * <p>To execute this pipeline, specify a local output file (if using the {@code DirectRunner}) or
 * output prefix on a supported distributed file system.
 *
 * <pre>{@code
 * --output=[YOUR_LOCAL_FILE | YOUR_OUTPUT_PREFIX]
 * }</pre>
 *
 * <p>The input file defaults to a public data set containing the text of of King Lear, by William
 * Shakespeare. You can override it and choose your own input with {@code --inputFile}.
 */
public class DirectEtl {

  /**
   * Concept #2: You can make your pipeline assembly code less verbose by defining your DoFns
   * statically out-of-line. This DoFn tokenizes lines of text into individual words; we pass it to
   * a ParDo in the pipeline.
   */
  static class ExtractWordsFn extends DoFn<KV<Integer, Person>, String> {
    private final Counter emptyLines = Metrics.counter(ExtractWordsFn.class, "emptyLines");
    private final Distribution lineLenDist =
        Metrics.distribution(ExtractWordsFn.class, "lineLenDistro");

    @ProcessElement
    public void processElement(@Element KV<Integer, Person> kvElement,
        OutputReceiver<String> receiver) {
      String element =
          kvElement.getValue().toString() + ":" + kvElement.getValue().getUuid() + ":" + kvElement
              .getValue().getNames();
      lineLenDist.update(element.length());
      if (element.trim().isEmpty()) {
        emptyLines.inc();
      }

      receiver.output(element);
      /*
      // Split the line into words.
      //String[] words = element.split(ExampleUtils.TOKENIZER_PATTERN, -1);
      String[] words = element.split("[^\\p{L}]+", -1);

      // Output each word encountered into the output PCollection.
      for (String word : words) {
        if (!word.isEmpty()) {
          receiver.output(word);
        }
      }
       */
    }
  }

  /** A SimpleFunction that converts a Word and Count into a printable string. */
  public static class FormatAsTextFn extends SimpleFunction<KV<String, Long>, String> {
    @Override
    public String apply(KV<String, Long> input) {
      return input.getKey() + ": " + input.getValue();
    }
  }

  /**
   * A PTransform that converts a PCollection containing lines of text into a PCollection of
   * formatted word counts.
   *
   * <p>Concept #3: This is a custom composite transform that bundles two transforms (ParDo and
   * Count) as a reusable PTransform subclass. Using composite transforms allows for easy reuse,
   * modular testing, and an improved monitoring experience.
   */
  public static class CountWords
      extends PTransform<PCollection<KV<Integer, Person>>, PCollection<KV<String, Long>>> {
    @Override
    public PCollection<KV<String, Long>> expand(PCollection<KV<Integer, Person>> lines) {

      // Convert lines of text into individual words.
      PCollection<String> words = lines.apply(ParDo.of(new ExtractWordsFn()));

      // Count the number of times each word occurs.
      PCollection<KV<String, Long>> wordCounts = words.apply(Count.perElement());

      return wordCounts;
    }
  }

  /**
   * Options supported by {@link DirectEtl}.
   */
  public interface DirectEtlOptions extends PipelineOptions {

    /**
     * By default, this reads from the MySQL DB `openmrs` at the default port on localhost.
     */
    @Description("JDBC URL input")
    @Default.String("jdbc:mysql://localhost:3306/openmrs")
    String getJdbcUrl();

    void setJdbcUrl(String value);

    @Description("JDBC MySQL driver class")
    @Default.String("com.mysql.cj.jdbc.Driver")
    String getJdbcDriverClass();

    void setJdbcDriverClass(String value);

    @Description("MySQL DB user")
    @Default.String("root")
    String getDbUser();

    void setDbUser(String value);

    @Description("MySQL DB user password")
    @Default.String("test")
    String getDbPassword();

    void setDbPassword(String value);

    // TODO(bashir2): Add options for writing to various data warehouses.
    /** The output Parquet file. */
    @Description("Path of the Parquet files to write to")
    @Required
    String getParquetOutput();

    void setParquetOutput(String value);
  }

  static <T extends BaseOpenmrsData> PCollection<KV<Integer, T>> getObjectTable(Pipeline p,
      DirectEtlOptions options, String query, Coder<T> coder, JdbcIO.RowMapper<KV<Integer, T>> rowMapper)
      throws CannotProvideCoderException {
    PCollection<KV<Integer, T>> rows = p.apply(
        "Read query " + query, JdbcIO.<KV<Integer, T>>read()
            .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
                options.getJdbcDriverClass(), options.getJdbcUrl())
                .withUsername(options.getDbUser())
                .withPassword(options.getDbPassword()))
            .withQuery(query)
            .withCoder(KvCoder.of(BigEndianIntegerCoder.of(), coder))
            .withRowMapper(rowMapper)
    );
    return rows;
  }

  static PCollection<KV<Integer, Person>> fetchPersons(Pipeline p,
      DirectEtlOptions options) throws CannotProvideCoderException {

    PCollection<KV<Integer, Person>> personTable = getObjectTable(
        p, options, "SELECT person_id, date_created, date_changed, voided, uuid from person",
        p.getCoderRegistry().getCoder(Person.class),
        new JdbcIO.RowMapper<KV<Integer, Person>>() {
          public KV<Integer, Person> mapRow(ResultSet resultSet) throws Exception {
            // TODO(bashir2): Can we do the mapping using Hibernate annotations of Person class?
            Person p = new Person();
            p.setId(resultSet.getInt("person_id"));
            p.setDateCreated(resultSet.getDate("date_created"));
            p.setDateChanged(resultSet.getDate("date_changed"));
            p.setVoided(resultSet.getBoolean("voided"));
            p.setUuid(resultSet.getString("uuid"));
            return KV.of(p.getId(), p);
          }
        });

    PCollection<KV<Integer, PersonName>> personNameTable = getObjectTable(
        p, options, "SELECT person_id, given_name, family_name, uuid from person_name",
        p.getCoderRegistry().getCoder(PersonName.class),
        new JdbcIO.RowMapper<KV<Integer, PersonName>>() {
          public KV<Integer, PersonName> mapRow(ResultSet resultSet) throws Exception {
            PersonName pn = new PersonName();
            Integer personId = resultSet.getInt("person_id");
            pn.setGivenName(resultSet.getString("given_name"));
            pn.setFamilyName(resultSet.getString("family_name"));
            pn.setUuid(resultSet.getString("uuid"));
            return KV.of(personId, pn);
          }
        });

    final TupleTag<Person> personTag = new TupleTag<>();
    final TupleTag<PersonName> nameTag = new TupleTag<>();
    PCollection<KV<Integer, CoGbkResult>> joinResults =
        KeyedPCollectionTuple.of(personTag, personTable)
            .and(nameTag, personNameTable)
            .apply(CoGroupByKey.create());

    PCollection<KV<Integer, Person>> personWithNames =
        joinResults.apply(
            "JoinPersonNames",
            ParDo.of(
                new DoFn<KV<Integer, CoGbkResult>, KV<Integer, Person>>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    KV<Integer, CoGbkResult> e = c.element();
                    Integer id = e.getKey();
                    Iterator<Person> personsIter = e.getValue().getAll(personTag).iterator();
                    Iterator<PersonName> namesIter = e.getValue().getAll(nameTag).iterator();
                    if (!personsIter.hasNext()) {
                      throw new IllegalArgumentException(
                          "Exactly one Person expected; found zero for person_id " + id);
                    }
                    Person person = personsIter.next();
                    // Note we cannot mutate the input in a DoFn and because of that we cannot even
                    // use copy constructor of Person because it may mutate some of the properties
                    // that are not initialized (e.g., addresses).
                    // For this (and other) reasons it is probably better to define new Beam
                    // friendly types and use schemas.
                    Person newPerson = new Person();
                    newPerson.setId(person.getId());
                    newPerson.setDateCreated(person.getDateCreated());
                    newPerson.setDateChanged(person.getDateChanged());
                    newPerson.setVoided(person.getVoided());
                    newPerson.setUuid((person.getUuid()));
                    newPerson.setNames(new HashSet<>());
                    if (personsIter.hasNext()) {
                      throw new IllegalArgumentException(
                          "Exactly one Person expected; found more for person_id " + id);
                    }
                    while (namesIter.hasNext()) {
                      PersonName name = namesIter.next();
                      PersonName newName = new PersonName();
                      newName.setGivenName(name.getGivenName());
                      newName.setFamilyName(name.getFamilyName());
                      newName.setUuid(name.getUuid());
                      newPerson.addName(newName);
                    }
                    c.output(KV.of(id, newPerson));
                  }
                }));
    return personWithNames;
  }

  static void runFlatten(DirectEtlOptions options) throws CannotProvideCoderException {
    Pipeline p = Pipeline.create(options);
    PCollection<KV<Integer, Person>> personTable = fetchPersons(p, options);


    personTable.apply(new CountWords())
        .apply(MapElements.via(new FormatAsTextFn()))
        .apply("WriteCounts", TextIO.write().to(options.getParquetOutput()));

    p.run().waitUntilFinish();
  }

  public static void main(String[] args) throws CannotProvideCoderException {
    DirectEtlOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(DirectEtlOptions.class);

    runFlatten(options);
  }
}
