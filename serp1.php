<?php
// establishing a DB connection 
static $dbconn; 
if(!isset($dbconn)) {
$config = parse_ini_file('.pm3_db_config.ini');
$conn_str = "host={$config['servername']} user={$config['username']} 
password={$config['password']} dbname={$config['dbname']}"; try{ 
$dbconn= pg_connect($conn_str); } catch (Exception $e) { 
echo $e->getMessage(); } 
} 
$q_phrase = pg_escape_string($_POST['q']);
$sql = "SELECT * FROM article WHERE to_tsvector(abstract) @@ to_tsquery('$q_phrase')"; 
$result = pg_query($dbconn, $sql); ?> 
<html lang="en"> <head> 
<title>Search Results</title> </head> 
<body>
<h2> article contain '<?php echo $q_phrase ?>' </h2> <ul> 
<?php
while ($row = pg_fetch_row($result)) { 
echo "<li>{$row[1]}</li>"; } 
?> 
</ul> 
</body> </html> 
