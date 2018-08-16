package com.dump.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryResponse;
import com.google.cloud.bigquery.TableResult;
import java.io.FileWriter;
import java.io.IOException;

import org.json.simple.JSONObject;
import java.util.UUID;
import org.jsoup.Jsoup;
import org.apache.commons.lang3.StringEscapeUtils;

public class ServianMain {
    public static void main(String... args) throws Exception {
        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

        QueryJobConfiguration preparedStatement =
                QueryJobConfiguration.newBuilder(
                        "WITH\n" +
                                "  FinalTBL AS (\n" +
                                "  WITH\n" +
                                "    IntermediateTBL AS (\n" +
                                "    WITH\n" +
                                "      FirstAndLastCommentTBL AS (\n" +
                                "      SELECT\n" +
                                "        DISTINCT `by` AS byCount,\n" +
                                "        MIN(time) AS firstcommenton,\n" +
                                "        MAX(time) AS lastcommenton\n" +
                                "      FROM\n" +
                                "        `bigquery-public-data.hacker_news.full_201510`\n" +
                                "      WHERE\n" +
                                "        `by` != 'null'\n" +
                                "        AND `type` = 'comment'\n" +
                                "      GROUP BY\n" +
                                "        `by`\n" +
                                "      ORDER BY\n" +
                                "        `by`\n" +
                                "      LIMIT\n" +
                                "        25000 ),\n" +
                                "      TotalCommentsTBL AS (\n" +
                                "      SELECT\n" +
                                "        DISTINCT `by`,\n" +
                                "        COUNT(`by`) AS totalcommentcount\n" +
                                "      FROM\n" +
                                "        `bigquery-public-data.hacker_news.full_201510`\n" +
                                "      WHERE\n" +
                                "        `by` != 'null'\n" +
                                "        AND `type` = 'comment'\n" +
                                "      GROUP BY\n" +
                                "        `by`\n" +
                                "      ORDER BY\n" +
                                "        `by`\n" +
                                "      LIMIT\n" +
                                "        25000 )\n" +
                                "    SELECT\n" +
                                "      `by` AS intermediateBy,\n" +
                                "      firstcommenton,\n" +
                                "      lastcommenton,\n" +
                                "      totalcommentcount\n" +
                                "    FROM\n" +
                                "      FirstAndLastCommentTBL\n" +
                                "    JOIN\n" +
                                "      TotalCommentsTBL\n" +
                                "    ON\n" +
                                "      FirstAndLastCommentTBL.byCount = TotalCommentsTBL.BY\n" +
                                "    ORDER BY\n" +
                                "      `by` )\n" +
                                "  SELECT\n" +
                                "    `by` AS finalby,\n" +
                                "    firstcommenton,\n" +
                                "    lastcommenton,\n" +
                                "    totalcommentcount,\n" +
                                "    text AS lastcommenttext\n" +
                                "  FROM\n" +
                                "    IntermediateTBL\n" +
                                "  JOIN\n" +
                                "    `bigquery-public-data.hacker_news.full_201510`\n" +
                                "  ON\n" +
                                "    IntermediateTBL.lastcommenton = `bigquery-public-data.hacker_news.full_201510`.time\n" +
                                "    AND `by` != 'null'\n" +
                                "    AND `type` = 'comment'\n" +
                                "  ORDER BY\n" +
                                "    `by`\n" +
                                "  LIMIT\n" +
                                "    25000 )\n" +
                                "SELECT\n" +
                                "  `by`,\n" +
                                "  firstcommenton,\n" +
                                "  lastcommenton,\n" +
                                "  totalcommentcount,\n" +
                                "  lastcommenttext,\n" +
                                "  text AS firstcommenttext\n" +
                                "FROM\n" +
                                "  FinalTBL\n" +
                                "JOIN\n" +
                                "  `bigquery-public-data.hacker_news.full_201510`\n" +
                                "ON\n" +
                                "  FinalTBL.firstcommenton = `bigquery-public-data.hacker_news.full_201510`.time\n" +
                                "  AND `by` != 'null'\n" +
                                "  AND `type` = 'comment'\n" +
                                "ORDER BY\n" +
                                "  `by`\n" +
                                "LIMIT\n" +
                                "  25000")
                        .setUseLegacySql(false)
                        .build();

        JobId jobId = JobId.of(UUID.randomUUID().toString());

        Job job = bigquery.create(JobInfo.newBuilder(preparedStatement).setJobId(jobId).build());

        job = job.waitFor();

        if (job == null) {
            throw new RuntimeException("Job no longer exists");
        } else if (job.getStatus().getError() != null) {
            throw new RuntimeException(job.getStatus().getError().toString());
        }

        QueryResponse response = bigquery.getQueryResults(jobId);

        TableResult result = job.getQueryResults();
        FileWriter jsonFile = null;
        try {
            jsonFile = new FileWriter("./dump.json");
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }

        for (FieldValueList row : result.iterateAll()) {
            JSONObject obj = new JSONObject();

            if(row.get("by") != null){
                String by = row.get("by").getStringValue();
                obj.put("by", by);
            }else{
                String by = "";
                obj.put("by", by);
            }

            if(row.get("firstcommenton") != null){
                long firstcommenton = row.get("firstcommenton").getLongValue();
                obj.put("firstcommenton", firstcommenton);
            }else{
                long firstcommenton = 0;
                obj.put("firstcommenton", firstcommenton);
            }

            if(row.get("lastcommenton") != null){
                long lastcommenton = row.get("lastcommenton").getLongValue();
                obj.put("lastcommenton", lastcommenton);
            }else{
                long lastcommenton = 0;
                obj.put("lastcommenton", lastcommenton);
            }

            if(row.get("totalcommentcount") != null){
                long totalcommentcount = row.get("totalcommentcount").getLongValue();
                obj.put("totalcommentcount", totalcommentcount);
            }else{
                long totalcommentcount = 0;
                obj.put("totalcommentcount", totalcommentcount);
            }

            if(row.get("lastcommenttext") != null){
                try{
                    String lastcommenttext = row.get("lastcommenttext").getStringValue();
                    lastcommenttext = Jsoup.parse(lastcommenttext).text();
                    lastcommenttext = org.apache.commons.lang3.StringEscapeUtils.unescapeJava(lastcommenttext);
                    obj.put("lastcommenttext", lastcommenttext);
                }catch(NullPointerException e){
                    obj.put("lastcommenttext", "");
                }

            }else{
                String lastcommenttext = "";
                obj.put("lastcommenttext", lastcommenttext);
            }

            if(row.get("firstcommenttext") != null){
                try {
                    String firstcommenttext = row.get("firstcommenttext").getStringValue();
                    firstcommenttext = Jsoup.parse(firstcommenttext).text();
                    firstcommenttext = org.apache.commons.lang3.StringEscapeUtils.unescapeJava(firstcommenttext);

                    obj.put("firstcommenttext", firstcommenttext);
                }
                catch(NullPointerException e){
                    obj.put("firstcommenttext", "");
                }
            }else{
                String firstcommenttext = "";
                obj.put("firstcommenttext", firstcommenttext);
            }

            jsonFile.write(obj.toJSONString());
            jsonFile.write(System.getProperty( "line.separator" ));
        }
    }
}