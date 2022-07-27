import psycopg2
from pickle import NONE

class ParXMLFile:

    def __init__(xml, path):
        xml.path = path
        xml.data = {}

    def parStrObj(xml,obj):
        data = ''
        if obj:
            data = obj[0].text[:255]
        return data

    def parAutList(xml,obj):
        data = []
        if obj:
            aut = obj[0].findall('Author')
            for author in aut:
                temp_dict = {}
                for ele in author.iter():
                    if ele.tag != 'Author':
                        temp_dict[ele.tag] = ele.text[:255]
                data.append(temp_dict)
        
        return data

    def parDateObj(xml, obj):
        data = []
        if obj:
            for ele in obj[0].iter():
                if ele.tag != 'DateCompleted':
                    data.append(ele.text)
        
        return '-'.join(data)

    def parKeywords(xml,obj):
        data = []
        if obj:
            kiwd = obj[0].findall("Keyword")
            for keyword in kiwd:
                for ele in keyword.iter():
                    data.append(ele.text[:255])
        return data

    def parData(xml):
        from lxml import etree
        tree = etree.parse(xml.path)
        root = tree.getroot()
        # required_tags = ["PMID", "DescriptorName", "Keyword", "Title", "ISSN", "DateCompleted", "AbstractText"]
        medart = root.findall("PubmedArticle")

        for article in medart:
            pmid = article.xpath("./MedlineCitation/PMID")[0].text
            article_title = article.xpath("./MedlineCitation/Article/ArticleTitle")
            author_list = article.xpath("./MedlineCitation/Article/AuthorList")
            mesh = article.xpath("./MedlineCitation/MeshHeadingList/MeshHeading/DescriptorName")
            kiwd = article.xpath("./MedlineCitation/KeywordList")
            journal_title = article.xpath("./MedlineCitation/Article/Journal/Title")
            journal_issn = article.xpath("./MedlineCitation/Article/Journal/ISSN")
            date_completed = article.xpath("./MedlineCitation/DateCompleted")
            abstract = article.xpath("./MedlineCitation/Article/Abstract/AbstractText")
            
            xml.data[pmid] = {
                'article_title': xml.parStrObj(article_title),
                'mesh': xml.parStrObj(mesh),
                'journal_title': xml.parStrObj(journal_title),
                'journal_issn': xml.parStrObj(journal_issn),
                'abstract': xml.parStrObj(abstract),
                'date': xml.parDateObj(date_completed),
            }

            # parsing author lists
            xml.data[pmid]['AuthorList'] = xml.parAutList(author_list)

            # parsing Keywrds
            xml.data[pmid]['keywords'] = xml.parKeywords(kiwd)

        return xml.data

class Database:
    def __init__(xml):
        xml.conn = None
        try:
            db_details = {
                'database': 'lchilumu',
                'user' : 'lchilumu',
                'password' : '001049338',
                'host' : '10.80.28.228',
                'port' : 5432
            }
            # Connecting to Database
            xml.connection = psycopg2.connect(**db_details)
            # Cursor Creation
            xml.cur = xml.connection.cursor()

        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: Connection can't be established")

    def closeConnection(xml):
        if xml.connection is not NONE:
            xml.connection.close()

    def creatingTables(xml):
        """
            This Method will create required Tables if not Exsists
        """
        author_table_query = """
            CREATE TABLE IF NOT EXISTS Author (
                id serial primary key, 
                lastname varchar(64), 
                forename varchar(64), 
                initials varchar(32),
                UNIQUE (lastname, forename, initials)
            ); 
        """
        xml.cur.execute(author_table_query)

        journal_table_query = """
            CREATE TABLE IF NOT EXISTS Journal (
                id serial primary key, 
                title varchar(255), 
                issn varchar(32),
                UNIQUE (title, issn)
            );
        """
        xml.cur.execute(journal_table_query)

        article_table_query = """
            CREATE TABLE IF NOT EXISTS Article (
                id serial primary key,
                title varchar(255),
                abstract text,
                journal_id integer REFERENCES Journal ( id ), 
                author integer [],
                UNIQUE (title)
            );
        """
        xml.cur.execute(article_table_query)

        citation_table_query = """
            CREATE TABLE IF NOT EXISTS MedlineCitation (
                pmid integer primary key,
                mesh varchar(255),
                keywords varchar(255) [],
                article_id integer REFERENCES Article(id),
                published_date DATE
            );
        """
        xml.cur.execute(citation_table_query)

        xml.connection.commit()

    def executeInsertQuery(xml, query, params):
        from psycopg2 import errors
        from psycopg2.errorcodes import UNIQUE_VIOLATION
        try:
            xml.cur.execute(query, params)
        except errors.lookup(UNIQUE_VIOLATION) as e:
            pass
        except Exception as err:
            print(query)
            print(params)
            print(err)
            pass
        finally:
            xml.connection.commit()
            xml.executeInsertQuery(query, params)

    def executeSelectQuery(xml, query, params):
        try:
            xml.cur.execute(query, params)
            record = xml.cur.fetchone()
            return record
        except Exception as err:
            print(query)
            print(params)
            print(err)
            return None
    
    def executeAnalysisSelectQuery(xml, query):
        try:
            xml.cur.execute(query)
            records = xml.cur.fetchall()
            return records
        except Exception as err:
            return None

    def insertXmlData(xml, data):
        """
            This method will insert the extracted data into Database
        """
        for pmid, article in data.items():
            authors_list = article['AuthorList']

            # Creating Authors
            author_ids = []
            for author in authors_list:
                query = """
                    INSERT INTO Author( lastname, forename, initials ) 
                        VALUES 
                            ( %s, %s, %s );
                """
                params = (author.get('LastName', ''), author.get('ForeName', ''), author.get('Initials', ''))
                xml.executeInsertQuery(query, params)

                # Fetching the saved author
                select_query = """
                    SELECT id FROM Author WHERE lastname = %s and forename = %s and initials = %s;
                """
                params = (author.get('LastName', ''), author.get('ForeName', ''), author.get('Initials', ''))
                id = xml.executeSelectQuery(select_query, params)
                author_ids.append(id[0])
             

            # creating Journals
            journal_id = []
            for journal in authors_list:
                query = """
                     INSERT INTO Journal( title, issn ) VALUES ( %s, %s );
                """
                params = (journal.get('journal_title', ''), journal.get('journal_issn', '') )
                xml.executeInsertQuery(query, params)

                # Fetching the saved Journal
                select_query = """
                  SELECT id FROM Journal WHERE title = %s and issn = %s;
                """
                params = (journal.get('journal_title', ''), journal.get('journal_issn', '') )
                id = xml.executeSelectQuery(select_query, params)
                journal_id.append(id[0])

            # Creating Articles 
            article_id = []
            for article in authors_list:
             query = """
                INSERT INTO Article(title, abstract, journal_id, authors)VALUES(%s, %s, %s, %s::integer[])

             """
             params = (article.get('article_title', ''), article.get('abstract', ''), str(journal_id), author_ids)
             xml.executeInsertQuery(query, params)

             # Fetching the saved Article
             select_query = "SELECT id FROM Article WHERE title=%s;"
             params =(article.get('article_title',''))
             id = xml.executeSelectQuery(select_query, params)
             article_id.append(id[0])

            # Creating The MedlineCitation
            query = """
                INSERT INTO MedlineCitation(pmid, mesh, keywords, article_id, published_date)
                VALUES(
                    %s,
                    %s,
                    %s::varchar(255)[],
                    %s,
                    %s

                );
            """
            params = (str(pmid), article['mesh'], article['keywords'], str(article_id),article['date'])
            xml.executeInsertQuery(query, params)

def saveFromXml():
    # Step-1
    print("Parsing XML")
    parser = ParXMLFile(path='pubmed22n0010.xml')
    data = parser.parData()

    # Step-2
    db_util = Database()
    if db_util.connection is not None:
        print("Creating tables")
        db_util.creatingTables()
        print("Inserting Data into DataBase")
        db_util.insertXmlData(data)
    return NONE

def runAnalysis():
    from pandas import DataFrame
    # Step:3 Analysis
    db_util = Database()
    query1 = """
        SELECT min(published_date) AS min, max(published_date) AS max from MedlineCitation;
    """
    q1_records = db_util.executeAnalysisSelectQuery(query1)
    # We are using Pands because of Decorated Output, or else it is no needed
    q1_df = DataFrame(q1_records)
    q1_df.columns = ['min', 'max']
    print("\nQuery-1\n")
    print(q1_df.to_string(index=False))

    query2 = """
        select count(Article.id) as cnt, Journal.issn, Journal.title from Article
            left join Journal on ( Journal.id =  Article.journal_id) 
                group by Journal.id order by cnt desc limit 5;
    """
    q2_records = db_util.executeAnalysisSelectQuery(query2)
    # We are using Pands because of Decorated Output, or else it is no needed
    q2_df = DataFrame(q2_records)
    q2_df.columns = ['cnt', 'issn', 'title']
    print("\nQuery-2\n")
    print(q2_df.to_string(index=False))

    query3 = """
        select count(pmid) as cnt, mesh, Article.title from MedlineCitation 
            inner join Article on (MedlineCitation.article_id = Article.id)
                group by mesh, Article.title  order by cnt desc limit 10;
    """
    q3_records = db_util.executeAnalysisSelectQuery(query3)
    # We are using Pands because of Decorated Output, or else it is no needed
    q3_df = DataFrame(q3_records)
    q3_df.columns = ['cnt', 'mesh', 'title']
    print("\nQuery-3\n")
    print(q3_df.to_string(index=False))

    # Closing Connection

    db_util.closeConnection()

def main():
    while True:
        choice = int(input("""
            Select From below Options:
                1. Parse Xml and Store into Database
                2. Run Analysis
                0. Exit
            Your Choice: 
            """))

        if choice == 1:
            saveFromXml()
        elif choice == 2:
            runAnalysis()
        elif choice == 0:
            break
        else:
            print("Please choose from above options")
        

if __name__ == "__main__": 
    main()
