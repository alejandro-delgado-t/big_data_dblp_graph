// Queries A.2
// Create constraint for uniqueness of journal
CREATE CONSTRAINT journal_unique IF NOT EXISTS
FOR (j:Journal)
REQUIRE (j.name, j.volume) IS UNIQUE;

// Load CSV and filter out rows with null or empty titles
LOAD CSV WITH HEADERS FROM 'file:///dblp_article_clean.csv' AS row FIELDTERMINATOR ';'
WITH row WHERE row.title IS NOT NULL AND trim(row.title) <> ''

// Create Paper node
MERGE (p:Paper {title: row.title})
SET p.last_m_date = row.m_date,
    p.doi = row.ee,
    p.url = row.url,
    p.abstract = row.abstract

// Create Journal node
MERGE (j:Journal {name: row.journal, volume: row.volume})
SET j.number = row.number

// Connect Paper -> Journal
MERGE (p)-[:PUBLISHED_IN]->(j)

// Create Time node
MERGE (t:Time {year: row.year})

// Connect Journal -> Time
MERGE (j)-[:HAS_PUBLICATION_DATE]->(t)

// Handle Authors
WITH row, p
WITH row, p, 
     split(row.author, '|') AS authors, 
     split(row.`author-orcid`, '|') AS orcids

UNWIND range(0, size(authors)-1) AS idx
WITH row, p, authors[idx] AS author_name, 
       CASE WHEN idx < size(orcids) THEN orcids[idx] ELSE NULL END AS author_orcid, 
       idx

MERGE (a:Author {name: author_name})
SET a.orcid = author_orcid

MERGE (p)-[:WRITTEN_BY]->(a)

// Add corresponding author
WITH row, p, a, idx
WHERE idx = 0
MERGE (p)-[:HAS_CORRESPONDING_AUTHOR]->(a)

// Add keyword relationships
WITH row, p
WITH p, apoc.convert.fromJsonList(row.keywords) AS keyword_list
UNWIND keyword_list AS kw
MERGE (t:Topic {name: kw})
MERGE (p)-[:HAS_KEYWORD]->(t);


// Note: Only the latest \texttt{number} is stored for each journal volume to avoid explosion in node count. The journal\_volume combination remains consistent.

// Create constraint for uniqueness in Inproceedings
CREATE CONSTRAINT inproceedings_unique IF NOT EXISTS
FOR (i:Inproceedings)
REQUIRE (i.booktitle, i.year) IS UNIQUE;

// Load CSV
LOAD CSV WITH HEADERS FROM 'file:///dblp_inproceedings_clean.csv' AS row FIELDTERMINATOR ';'
WITH row
WHERE row.title IS NOT NULL AND trim(row.title) <> ''

// Create Paper node
MERGE (p:Paper {title: row.title})
SET p.last_m_date = row.m_date,
    p.doi = row.ee,
    p.url = row.url,
    p.abstract = row.abstract

// Create Inproceedings node based on booktitle and year
MERGE (i:Inproceedings {booktitle: row.booktitle, year: row.year})

// Connect Paper -> Inproceedings
MERGE (p)-[:PUBLISHED_IN]->(i)

// Create Time node
MERGE (t:Time {year: row.year})

// Connect Inproceedings -> Time
MERGE (i)-[:HAS_PUBLICATION_DATE]->(t)

// Connect Inproceedings -> Type
MERGE (type:Type {name: row.type})
MERGE (i)-[:HAS_TYPE]->(type)

// Connect Inproceedings -> Venue
MERGE (v:Venue {name: row.venue})
MERGE (i)-[:HAS_VENUE]->(v)

// Handle Authors
WITH row, p
WITH row, p, 
     split(row.author, '|') AS authors, 
     split(row.`author-orcid`, '|') AS orcids

UNWIND range(0, size(authors)-1) AS idx
WITH row, p, authors[idx] AS author_name, 
       CASE WHEN idx < size(orcids) THEN orcids[idx] ELSE NULL END AS author_orcid, 
       idx

MERGE (a:Author {name: author_name})
SET a.orcid = author_orcid

MERGE (p)-[:WRITTEN_BY]->(a)

// Connect Paper -> First Author as CORRESPONDING_AUTHOR
WITH row, p, a, idx
WHERE idx = 0
MERGE (p)-[:HAS_CORRESPONDING_AUTHOR]->(a)

// Add keyword relationships
WITH row, p
WITH p, apoc.convert.fromJsonList(row.keywords) AS keyword_list
UNWIND keyword_list AS kw
MERGE (t:Topic {name: kw})
MERGE (p)-[:HAS_KEYWORD]->(t);

// Load reviewer-paper edges
LOAD CSV WITH HEADERS FROM 'file:///review_edges.csv' AS row FIELDTERMINATOR ';'
WITH row
WHERE row.reviewer IS NOT NULL AND row.paper IS NOT NULL

MATCH (a:Author {name: row.reviewer})
MATCH (p:Paper {title: row.paper})
MERGE (a)-[:REVIEWED]->(p);

// Create citations between papers that share at least 6 keywords
MATCH (p1:Paper)-[:HAS_KEYWORD]->(k:Topic)<-[:HAS_KEYWORD]-(p2:Paper)
WHERE p1 <> p2
WITH p1, p2, COUNT(DISTINCT k) AS shared_keywords
WHERE shared_keywords >= 6
// Use a simpler condition to ensure direction
AND id(p1) < id(p2)
MERGE (p1)-[:CITES]->(p2);

// Question A.3
// Create constraints for new node types
CREATE CONSTRAINT organization_name_unique IF NOT EXISTS
FOR (o:Organization) REQUIRE o.name IS UNIQUE;

CREATE CONSTRAINT review_unique IF NOT EXISTS
FOR (r:Review) REQUIRE (r.reviewer_name, r.paper_title) IS UNIQUE;


// Create organization nodes and connect authors randomly
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

// Connect authors to random organizations
MATCH (a:Author)
WITH a, rand() AS r
MATCH (o:Organization)
WITH a, o, r ORDER BY r
LIMIT 1
MERGE (a)-[:AFFILIATED_WITH]->(o);


MATCH (a:Author)-[r:REVIEWED]->(p:Paper)
WITH a, p

WITH a, p,
  CASE WHEN rand() > 0.3 THEN 'accept' ELSE 'reject' END AS decision,
  'This paper ' + 
  CASE WHEN rand() > 0.5 
    THEN 'presents an interesting approach to ' 
    ELSE 'proposes a novel method for ' 
  END + 
  'the problem. ' +
  CASE WHEN rand() > 0.3 
    THEN 'The methodology is sound and the results are convincing. ' 
    ELSE 'The experimental evaluation could be more thorough. ' 
  END +
  CASE WHEN rand() > 0.7 
    THEN 'Overall, a strong contribution to the field.' 
    ELSE 'Some claims require better justification.' 
  END AS content

MERGE (r:Review {
  reviewer_name: a.name,
  paper_title: p.title
})
SET r.content = content,
    r.decision = decision

MERGE (a)-[:WROTE_REVIEW]->(r)
MERGE (r)-[:REVIEWS]->(p)

WITH a, p
MATCH (a)-[old:REVIEWED]->(p)
DELETE old;

// Calculate paper acceptance based on majority decision

MATCH (p:Paper)<-[:REVIEWS]-(r:Review)
WITH p, COLLECT(r.decision) AS decisions
WITH p, 
     SIZE([d IN decisions WHERE d = 'accept']) AS accept_count,
     SIZE(decisions) AS total_count
WHERE total_count > 0
SET p.accepted = CASE 
  WHEN 1.0 * accept_count / total_count >= 0.5 THEN true 
  ELSE false 
END;

// Create indexes to improve query performance

CREATE INDEX review_decision_idx IF NOT EXISTS FOR (r:Review) ON (r.decision);
CREATE INDEX organization_type_idx IF NOT EXISTS FOR (o:Organization) ON (o.type);
CREATE INDEX paper_accepted_idx IF NOT EXISTS FOR (p:Paper) ON (p.accepted);

// Question B
// Find the top 3 most cited papers for each conference
MATCH (conf:Inproceedings)-[:HAS_TYPE]->(t:Type {name: 'conference'})
MATCH (p:Paper)-[:PUBLISHED_IN]->(conf)
MATCH (p)<-[c:CITES]-()
WITH conf.booktitle AS conference, conf.year AS year, p, COUNT(c) AS citations
ORDER BY conference, year, citations DESC
WITH conference, year, COLLECT({paper: p.title, citations: citations}) AS papers
RETURN conference, year, 
       [x IN papers[0..3] | {title: x.paper, citation_count: x.citations}] AS top_cited_papers;

// Find authors who published in at least 4 editions of the same conference
MATCH (conf:Inproceedings)-[:HAS_TYPE]->(t:Type {name: 'conference'})
MATCH (p:Paper)-[:PUBLISHED_IN]->(conf)
MATCH (p)-[:WRITTEN_BY]->(a:Author)
WITH conf.booktitle AS conference, conf.year AS year, a
WITH conference, a, COUNT(DISTINCT year) AS editions
WHERE editions >= 4
WITH conference, COLLECT(a.name) AS community_members, COUNT(a) AS community_size
RETURN conference, community_size, community_members
ORDER BY community_size DESC;

// Compute journal impact factor based on recent publications and citations
// Revised impact factor calculation
// Step 1: Get the maximum year from Time nodes as "current year"
MATCH (t:Time)
WITH MAX(t.year) AS current_year

// Step 2: For each journal, find papers published in the previous two years
MATCH (j:Journal)<-[:PUBLISHED_IN]-(p:Paper)-[:HAS_PUBLICATION_DATE]->(t:Time)
WHERE t.year >= current_year - 2 AND t.year < current_year
WITH j, current_year, COLLECT(p) AS papers, COUNT(p) AS paper_count

// Step 3: Count citations to these papers
UNWIND papers AS paper
OPTIONAL MATCH (paper)<-[c:CITES]-()
WITH j, current_year, paper_count, COUNT(c) AS citation_count

// Step 4: Calculate impact factor
WITH j, current_year, paper_count, citation_count,
     CASE WHEN paper_count > 0 THEN 1.0 * citation_count / paper_count ELSE 0 END AS impact_factor
     
RETURN j.name AS journal, current_year, paper_count AS papers_last_two_years, 
       citation_count AS citations, impact_factor
ORDER BY impact_factor DESC;

// Compute h-index for authors based on citations of their papers
MATCH (a:Author)
MATCH (p:Paper)-[:WRITTEN_BY]->(a)
OPTIONAL MATCH (p)<-[c:CITES]-()
WITH a, p, COUNT(c) AS citations
ORDER BY a.name, citations DESC

WITH a, COLLECT(citations) AS citation_counts
WITH a, citation_counts,
     REDUCE(h = 0, i IN RANGE(0, SIZE(citation_counts)-1) |
       CASE WHEN i < citation_counts[i] THEN i+1 ELSE h END
     ) AS h_index

RETURN a.name AS author, h_index, citation_counts[0..5] AS top_citations
ORDER BY h_index DESC, a.name
LIMIT 20;

// Question C
// Add random weights to CITES relationships
MATCH ()-[r:CITES]->()
SET r.weight = 0.5 + rand() * 0.5
RETURN COUNT(r) AS relationships_updated;


// Create graph projection with relationship weights
CALL gds.graph.project(
  'paperCitationGraph',
  'Paper',
  {
    CITES: {
      orientation: 'NATURAL',
      properties: ['weight']
    }
  }
)
YIELD graphName, nodeCount, relationshipCount;

// Run PageRank with relationship weights
CALL gds.pageRank.stream('paperCitationGraph', {
  maxIterations: 20,
  dampingFactor: 0.85,
  relationshipWeightProperty: 'weight'
})
YIELD nodeId, score
WITH gds.util.asNode(nodeId) AS paper, score
RETURN paper.title AS title, score AS pageRank
ORDER BY pageRank DESC
LIMIT 10;