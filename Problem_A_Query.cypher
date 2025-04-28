// =====================================================
//  DBLP GRAPH INGEST - Problem A
// -----------------------------------------------------
//  SECTION A.2 – JOURNAL PAPERS (dblp_article_clean.csv)
// ──────────────────────────────────────────────────────

// We add a uniqueness constraint on (name, volume) so we
//     never end up with duplicate Journal nodes when the script
//     is re‑run.  "IF NOT EXISTS" keeps it idempotent.
CREATE CONSTRAINT journal_unique IF NOT EXISTS
FOR (j:Journal)
REQUIRE (j.name, j.volume) IS UNIQUE;

// We read the CSV, telling Neo4j that fields are separated by
//     semicolons.  Rows with missing or blank titles are ignored
//     because a paper without a title isn’t helpful to us.
LOAD CSV WITH HEADERS FROM 'file:///dblp_article_clean.csv' AS row FIELDTERMINATOR ';'
WITH row WHERE row.title IS NOT NULL AND trim(row.title) <> ''

// We create (or reuse) a Paper node per *title*.  Extra metadata
//     (mdate, DOI, URL, abstract) is stored so later queries don’t
//     need to go back to the raw files.
MERGE (p:Paper {title: row.title})
SET p.last_mdate = row.mdate,
    p.doi        = row.ee,
    p.url        = row.url,
    p.abstract   = row.abstract

// For each (journal, volume) pair we merge a Journal node and
//     keep the latest issue number we’ve seen.  We decided to do
//     that because many journals reuse the same volume number for
//     multiple issues and we only care about the most recent one.
MERGE (j:Journal {name: row.journal, volume: row.volume})
SET j.number = row.number

// We connect every Paper to its Journal via [:PUBLISHED_IN].
MERGE (p)-[:PUBLISHED_IN]->(j)

// We attach each Journal to a Time node (year granularity).
//     That way, time‑series queries run fast and stay clean.
MERGE (t:Time {year: row.year})
MERGE (j)-[:HAS_PUBLICATION_DATE]->(t)

// Author handling – we split the author string on "|" so we can
//     iterate over individual names.  We keep ORCIDs when they’re
//     present, otherwise we leave the property null.
WITH row, p,
     split(row.author, '|')         AS authors,
     split(row.`author-orcid`, '|') AS orcids
UNWIND range(0, size(authors)-1) AS idx
WITH row, p,
     authors[idx]                                                       AS author_name,
     CASE WHEN idx < size(orcids) THEN orcids[idx] ELSE NULL END        AS author_orcid,
     idx
MERGE (a:Author {name: author_name})
SET a.orcid = author_orcid
MERGE (p)-[:WRITTEN_BY]->(a)

// We treat the first author in the list as the corresponding
//     author—simple heuristic but good enough for demo purposes.
WITH row, p, a, idx WHERE idx = 0
MERGE (p)-[:HAS_CORRESPONDING_AUTHOR]->(a)

// Keyword handling – we decode the JSON list with APOC, then we
//     connect the Paper to Topic nodes (creating them on demand).
WITH row, p
WITH p, apoc.convert.fromJsonList(row.keywords) AS keyword_list
UNWIND keyword_list AS kw
MERGE (t:Topic {name: kw})
MERGE (p)-[:HAS_KEYWORD]->(t);

// ──────────────────────────────────────────────────────
//  SECTION A.2 CONFERENCE PAPERS (dblp_inproceedings_clean.csv)
// ──────────────────────────────────────────────────────

// We set up a uniqueness constraint so each (booktitle, year)
//     pair gives us exactly one Inproceedings node.
CREATE CONSTRAINT inproceedings_unique IF NOT EXISTS
FOR (i:Inproceedings)
REQUIRE (i.booktitle, i.year) IS UNIQUE;

// Same CSV‑loading pattern, we skip blank titles again.
LOAD CSV WITH HEADERS FROM 'file:///dblp_inproceedings_clean.csv' AS row FIELDTERMINATOR ';'
WITH row WHERE row.title IS NOT NULL AND trim(row.title) <> ''

// Paper node.
MERGE (p:Paper {title: row.title})
SET p.last_mdate = row.mdate,
    p.doi        = row.ee,
    p.url        = row.url,
    p.abstract   = row.abstract

// Inproceedings node.
MERGE (i:Inproceedings {booktitle: row.booktitle, year: row.year})

// Paper → Inproceedings link.
MERGE (p)-[:PUBLISHED_IN]->(i)

// We wire the conference instance to its Time node (year).
MERGE (t:Time {year: row.year})
MERGE (i)-[:HAS_PUBLICATION_DATE]->(t)

// We decided to store the submission type (full/short/etc.)
//     as its own node so we can quickly group papers later.
MERGE (type:Type {name: row.type})
MERGE (i)-[:HAS_TYPE]->(type)

// Venue node lets us query papers by conference location or
//     series host without string‑matching.
MERGE (v:Venue {name: row.venue})
MERGE (i)-[:HAS_VENUE]->(v)

// Author parsing – identical to the journal branch.
WITH row, p,
     split(row.author, '|')         AS authors,
     split(row.`author-orcid`, '|') AS orcids
UNWIND range(0, size(authors)-1) AS idx
WITH row, p,
     authors[idx]                                                       AS author_name,
     CASE WHEN idx < size(orcids) THEN orcids[idx] ELSE NULL END        AS author_orcid,
     idx
MERGE (a:Author {name: author_name})
SET a.orcid = author_orcid
MERGE (p)-[:WRITTEN_BY]->(a)

// First author → corresponding.
WITH row, p, a, idx WHERE idx = 0
MERGE (p)-[:HAS_CORRESPONDING_AUTHOR]->(a)

// Keywords.
WITH row, p
WITH p, apoc.convert.fromJsonList(row.keywords) AS keyword_list
UNWIND keyword_list AS kw
MERGE (t:Topic {name: kw})
MERGE (p)-[:HAS_KEYWORD]->(t);

// ──────────────────────────────────────────────────────
//  SECTION – REVIEWS & CITATIONS
// ──────────────────────────────────────────────────────

// We import reviewer→paper pairs and attach them directly with
//     [:REVIEWED].  This gives us a foothold to build richer review
//     entities later.
LOAD CSV WITH HEADERS FROM 'file:///review_edges.csv' AS row FIELDTERMINATOR ';'
WITH row WHERE row.reviewer IS NOT NULL AND row.paper IS NOT NULL
MATCH (a:Author {name: row.reviewer})
MATCH (p:Paper  {title: row.paper})
MERGE (a)-[:REVIEWED]->(p);

// Citations – our heuristic: if two papers share at least
//     six keywords, the earlier one cites the later one.  We add
//     an extra id‑based tie‑breaker so the direction is deterministic.
MATCH (p1:Paper)-[:HAS_KEYWORD]->(k:Topic)<-[:HAS_KEYWORD]-(p2:Paper)
WHERE p1 <> p2
WITH p1, p2, COUNT(DISTINCT k) AS shared_keywords
WHERE shared_keywords >= 6
  AND ( p1.last_mdate < p2.last_mdate
        OR (p1.last_mdate = p2.last_mdate AND id(p1) < id(p2)) )
MERGE (p1)-[:CITES]->(p2);

// ──────────────────────────────────────────────────────
//  SECTION A.3 – SYNTHETIC AFFILIATIONS & FULL REVIEWS
// ──────────────────────────────────────────────────────

//  We set up constraints for Organization and Review nodes so we
//     can’t accidentally duplicate them.
CREATE CONSTRAINT organization_name_unique IF NOT EXISTS
FOR (o:Organization) REQUIRE o.name IS UNIQUE;

CREATE CONSTRAINT review_unique IF NOT EXISTS
FOR (r:Review) REQUIRE (r.reviewer_name, r.paper_title) IS UNIQUE;

//  We create a small list of universities and companies, then we
//     MERGE a node for each one with a type label.  Having both
//     academia and industry lets us demo queries on collaboration.
WITH [
  'Stanford University', 'MIT', 'Berkeley', 'Harvard', 'Oxford',
  'Cambridge', 'ETH Zurich', 'Technical University of Munich',
  'University of Tokyo', 'Tsinghua University'
] AS universities,
[
  'Google', 'Microsoft', 'Apple', 'Meta', 'Amazon',
  'IBM', 'Intel', 'Nvidia', 'Baidu', 'Samsung'
] AS companies
UNWIND universities AS uni_name
MERGE (o:Organization {name: uni_name, type: 'university'})

WITH companies
UNWIND companies AS comp_name
MERGE (o:Organization {name: comp_name, type: 'company'});

// For demo simplicity we randomly assign exactly one affiliation
//     to every author.  In production we’d of course use real data.
MATCH (a:Author)
WITH a, rand() AS r
MATCH (o:Organization)
WITH a, o, r ORDER BY r LIMIT 1
MERGE (a)-[:AFFILIATED_WITH]->(o);

// Now we replace the simple [:REVIEWED] edge with a full Review
//     node that stores text + accept/reject.  We chose a 70/30 accept
//     ratio and some templated prose so we can explore review text in
//     NLP demos.
MATCH (a:Author)-[r:REVIEWED]->(p:Paper)
WITH a, p
WITH a, p,
  CASE WHEN rand() > 0.3 THEN 'accept' ELSE 'reject' END AS decision,
  'This paper ' +
  CASE WHEN rand() > 0.5 THEN 'presents an interesting approach to ' ELSE 'proposes a novel method for ' END +
  'the problem. ' +
  CASE WHEN rand() > 0.3 THEN 'The methodology is sound and the results are convincing. ' ELSE 'The experimental evaluation could be more thorough. ' END +
  CASE WHEN rand() > 0.7 THEN 'Overall, a strong contribution to the field.' ELSE 'Some claims require better justification.' END AS content
MERGE (rev:Review { reviewer_name: a.name, paper_title: p.title })
SET rev.content  = content,
    rev.decision = decision
MERGE (a)-[:WROTE_REVIEW]->(rev)
MERGE (rev)-[:REVIEWS]->(p)
WITH a, p
MATCH (a)-[old:REVIEWED]->(p)
DELETE old; // we don’t need the placeholder edge anymore

// We flag each Paper as accepted or rejected based on majority
//     vote across its Review nodes.  >=50% "accept" means accepted.
MATCH (p:Paper)<-[:REVIEWS]-(rev:Review)
WITH p, COLLECT(rev.decision) AS decisions
WITH p,
     SIZE([d IN decisions WHERE d = 'accept']) AS accept_count,
     SIZE(decisions) AS total_count
WHERE total_count > 0
SET p.accepted = CASE WHEN 1.0 * accept_count / total_count >= 0.5 THEN true ELSE false END;

// Finally we create three single‑property indexes so lookups on
//     review decision, organization type, and acceptance status stay
//     snappy as the dataset grows.
CREATE INDEX review_decision_idx    IF NOT EXISTS FOR (rev:Review)     ON (rev.decision);
CREATE INDEX organization_type_idx IF NOT EXISTS FOR (o:Organization) ON (o.type);
CREATE INDEX paper_accepted_idx     IF NOT EXISTS FOR (p:Paper)       ON (p.accepted);

// ——————————————————————  FINALE  ——————————————————————
