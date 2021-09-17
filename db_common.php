<?php

require_once('common.php');
require_once('logs.php');

/**
 * Массив в XML
 *
 * @param array  $data
 * @param object $xml
 */
function array_to_xml($data, &$xml){
	if($data){
		if(is_object($data) || is_array($data)){
			foreach($data as $key => $value){
				if(is_numeric($key)){
					$key = 'i'; // . $key; //dealing with <0/>..<n/> issues
				}

				if(is_array($value) || is_object($value)){
					$subNode = $xml->addChild($key);
					array_to_xml($value, $subNode);

				}else{
					$xml->addChild("$key", htmlspecialchars("$value"));
				}
			}

		}else{
			$xml->addChild("item", htmlspecialchars("$data"));
		}
	}
}

/**
 * Подготовка стандартного сообщения об ошибке выполнения SQL запроса
 *
 * @param string     $SQL
 * @param array|null $params
 * @param resource   $result
 * @param resource   $conn
 *
 * @return array
 */
function getSQLError(string $SQL, array $params = null, $result = null, $conn = null)
: array{
	$error['message'] = 'Ошибка выполнения запроса:';
	$error['SQL'] = $SQL;

	if($params){
		$error['params'] = $params;
	}

	$error['error'] = [];

	if($result === false && $conn){
		$result = pg_get_result($conn);
	}
	if($result){
		$error['error'][] = pg_result_error($result);
	}

	if($conn){
		$error['error'][] = pg_connection_status($conn);
		$error['error'][] = pg_last_error($conn);
	}

	return $error;
}

function setSQLDebugInfo($SQL, $params = null){
	global $answer;
	if(getBoolParamByName('debug')){
		if(!isset($answer['debug'])){
			$answer['debug'] = [];
		}

		$debug['SQL'] = $SQL;
		$debug['params'] = $params;
		$answer['debug'][] = $debug;
	}
}

/**
 * Вывод ошибки в поток вывода
 *
 * @param mixed       	 $SQL     Текст запроса
 * @param array|null 	[$params] Параметры запроса
 * @param resource|null	[$result] Запрос
 * @param resource|null	[$conn]   Соединение
 */
function Error_SQL_Answer($SQL, array $params = null, $result = null, $conn = null){
	$error = getSQLError($SQL, $params, $result, $conn);

	if($result){
		pg_free_result($result);
	}

	if($conn){
		pg_close($conn);
	}

	Error_Answer($error);
}

/**
 * Открывает соединение с БД
 *
 * @param string [$connectStr] Строка - параметры соединения с БД
 *
 * @return resource
 */
//'host=127.0.0.1 dbname=' . $dbName . $searchPath . ($userName ? ' user=' . $userName : '') . ($pass ? ' password=' . $pass : '') . ($searchPath ? ' csearch_path=' . $searchPath : '')
function _openConnect($connectStr = ''){ // User ID=root;Password=myPassword
	if(!$connectStr){
		$connectStr = 'host=127.0.0.1 dbname=osm user=osm password=Qwerty21';
	}

	$conn = pg_connect($connectStr, PGSQL_CONNECT_FORCE_NEW);
	if(!$conn){   //Ошибка открытия соединения с БД
		Error_SQL_Answer('ОШИБКА открытия соединение с БД', null, null, $conn);
	}

	return $conn;
}

/**
 * Закрываем соединение с БД
 *
 * @param resource $conn Указатель на соединение
 */
function _closeConnect($conn){
	pg_close($conn);
}

/**
 * Выполняет запрос в БД
 *
 * @param resource $conn
 * @param string   $SQL
 * @param array    $SQL_Params
 *
 * @return array
 */
function _getQueryResult($conn, string $SQL, array $SQL_Params)
: array{
	$query = pg_query_params($conn, $SQL, $SQL_Params);

	if(!empty($query)){
		return pg_fetch_all($query);
	}

	Error_SQL_Answer($SQL, $SQL_Params, $query, $conn);
}

/**
 * Выполнение скалярного запроса
 *
 * @param string $SQL
 * @param array    [$Params = []]
 * @param resource [$conn = null]
 *
 * @return false|mixed
 * @throws ErrorException
 */
function execSingleValQuery(string $SQL, array $Params = [], $conn= null){
	$_closeConn = false;
	if(!$conn){
		$conn = _openConnect();
		$_closeConn = true;
	}

	$query = null;
	try{
		$query = pg_query_params($conn, $SQL, $Params);
		if(!empty($query)){
			$row = pg_fetch_row($query);
			return $row[0];
		}

		return false;

	}catch(Exception $err){
		//Тут бы записать куда ошибку...
		throw new ErrorException(getSQLError($SQL, $Params, $query, $conn), -100);

	}finally{
		if(!empty($query)){
			pg_free_result($query);
		}
		$_closeConn && _closeConnect($conn);
	}
}

/**
 * Выполнение однострочного запроса
 *
 * @param string $SQL
 * @param array    [$Params = []]
 * @param resource [$conn = null]
 *
 * @return array|false
 * @throws \ErrorException
 */
function execSingleRowQuery(string $SQL, array $Params = [], $conn= null){
	$_closeConn = false;
	if(!$conn){
		$conn = _openConnect();
		$_closeConn = true;
	}

	$query = null;
	try{
		$query = pg_query_params($conn, $SQL, $Params);
		if(!empty($query)){
			return pg_fetch_row($query);
		}

		return false;

	}catch(Exception $err){
		//Тут бы записать куда ошибку...
		throw new ErrorException(getSQLError($SQL, $Params, $query, $conn), -200);

	}finally{
		if(!empty($query)){
			pg_free_result($query);
		}
		$_closeConn && _closeConnect($conn);
	}
}

/**
 * Выполнение многострочного запроса
 *
 * @param string $SQL
 * @param array    [$Params = []]
 * @param resource [$conn = null]
 *
 * @return array|false
 * @throws \ErrorException
 */
function execMultiRowQuery(string $SQL, array $Params = [], $conn= null){
	$_closeConn = false;
	if(!$conn){
		$conn = _openConnect();
		$_closeConn = true;
	}

	$query = null;
	try{
		$query = pg_query_params($conn, $SQL, $Params);
		if(!empty($query)){
			return pg_fetch_all($query);
		}

		return false;

	}catch(Exception $err){
		//Тут бы записать куда ошибку...
		throw new ErrorException(getSQLError($SQL, $Params, $query, $conn), -300);

	}finally{
		if(!empty($query)){
			pg_free_result($query);
		}
		$_closeConn && _closeConnect($conn);
	}
}
