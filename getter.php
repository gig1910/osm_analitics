<?php
ini_set('error_reporting', E_ALL);
ini_set('display_errors', 1);
ini_set('display_startup_errors', 1);

ini_set('memory_limit', '-1');

header('Content-Language: ru-RU');
//header('Content-Encoding: windows-1251');
//header('Date: ' . date());
//header('Expires: ' . date());
header('Pragma: no-cache');
header('Content-Type: application/json');

require_once('common.php');
require_once('db_common.php');

global $answer;

$mode = getIntParamByName('mode', 0);

/** Границы, по котором будут "обрезаны" дороги (координаты "скользящего окна") */
$bounds = [];
$tmp = getStrParamByName('bounds');
if($tmp && $tmp !== ''){
	$tmp = explode('|', $tmp);
	if(is_array($tmp)){
		foreach($tmp as $item){
			$val = toFloat($item);
			if(!is_nan($val)){
				$bounds[] = $val;
			}else{
				$bounds = null;
				break;
			}
		}
	}

}else{
	$bounds = null;
}

$SQL = '';
$SQL_Params = [];

switch($mode){
	case 0: //Дороги, скоростной режим
		if(count($bounds) > 0){
			$SQL = '
WITH T(way, P, tags) as (
    select ST_AsGeoJSON(ST_ShiftLongitude(ST_Transform(way, 4326)), 6)::JSONB            as line,
           ST_ShiftLongitude(ST_Transform(ST_ClosestPoint(way, ST_Centroid(way)), 4326)) as P,
           tags
    from osm.planet_osm_line
    where highway in (\'motorway\',\'trunk\',\'primary\',\'secondary\',\'tertiary\',\'tertiary_link\',\'unclassified\',\'living_street\',\'residential\',\'motorway_link\',\'trunk_link\',\'primary_link\',\'secondary_link\')
      and ST_Intersects(way, ST_Transform(ST_SetSRID(ST_MakeBox2D(
                                                             ST_MakePoint($' . (count($SQL_Params) + 1) . '::NUMERIC, $' . (count($SQL_Params) + 2) . '::NUMERIC),
                                                             ST_MakePoint($' . (count($SQL_Params) + 3) . '::NUMERIC, $' . (count($SQL_Params) + 4) . '::NUMERIC)),
                                                     4674), 3857))
)
select jsonb_build_object(
               \'mode\', 0,
               \'dicts\',
               jsonb_build_object(\'dict1\',
                                  json_build_array(array(select distinct tags -> \'maxspeed\' from T ORDER BY 1)) -> 0)::JSONB,
               \'geoms\', jsonb_build_object(
                       \'type\', \'FeatureCollection\',
                       \'features\', jsonb_build_array(array(
                        select jsonb_build_object(
                                       \'type\', \'Feature\',
                                       \'geometry\', T.way::JSONB,
                                       \'properties\',
                                       jsonb_build_object(
                                               \'l\', tags -> \'maxspeed\',
                                               \'p_lat\', ST_Y(P)::NUMERIC,
                                               \'p_lng\', ST_X(P)::NUMERIC
                                       )::JSONB
                                   )
                        from T
                    )) -> 0)::JSONB
           )::JSONB;';

			$SQL_Params[] = $bounds[1];
			$SQL_Params[] = $bounds[0];
			$SQL_Params[] = $bounds[3];
			$SQL_Params[] = $bounds[2];

		}else{
			Error_Answer('Обязательно указывать зону ограничения в запросе');
		}
		break;

	case 1: //Дороги, количество полос
		if(count($bounds) > 0){
			$SQL = '
WITH T(way, P, tags) as (
    select ST_AsGeoJSON(ST_ShiftLongitude(ST_Transform(way, 4326)), 6)::JSONB            as way,
           ST_ShiftLongitude(ST_Transform(ST_ClosestPoint(way, ST_Centroid(way)), 4326)) as P,
           tags
    from osm.planet_osm_line
    where highway in (\'motorway\',\'trunk\',\'primary\',\'secondary\',\'tertiary\',\'tertiary_link\',\'unclassified\',\'living_street\',\'residential\',\'motorway_link\',\'trunk_link\',\'primary_link\',\'secondary_link\')
      and ST_Intersects(way, ST_Transform(ST_SetSRID(ST_MakeBox2D(
                                                             ST_MakePoint($' . (count($SQL_Params) + 1) . '::NUMERIC, $' . (count($SQL_Params) + 2) . '::NUMERIC),
                                                             ST_MakePoint($' . (count($SQL_Params) + 3) . '::NUMERIC, $' . (count($SQL_Params) + 4) . '::NUMERIC)),
                                                     4674), 3857))
)
select jsonb_build_object(
               \'mode\', 1,
               \'dicts\',
               jsonb_build_object(\'dict1\',
                                  json_build_array(array(select distinct tags -> \'lanes\' from T ORDER BY 1)) -> 0)::JSONB,
               \'geoms\', jsonb_build_object(
                       \'type\', \'FeatureCollection\',
                       \'features\', jsonb_build_array(array(
                        select jsonb_build_object(
                                       \'type\', \'Feature\',
                                       \'geometry\', T.way::JSONB,
                                       \'properties\',
                                       jsonb_build_object(
                                               \'l\', tags -> \'lanes\',
                                               \'p_lat\', ST_Y(P)::NUMERIC,
                                               \'p_lng\', ST_X(P)::NUMERIC
                                       )::JSONB
                                   )
                        from T
                    )) -> 0)::JSONB
           )::JSONB;';

			$SQL_Params[] = $bounds[1];
			$SQL_Params[] = $bounds[0];
			$SQL_Params[] = $bounds[3];
			$SQL_Params[] = $bounds[2];

		}else{
			Error_Answer('Обязательно указывать зону ограничения в запросе');
		}
		break;

	case 3: //Дороги, освещённость
		if(count($bounds) > 0){
			$SQL = '
WITH T(way, P, tags) as (
    select way                                                        as way,
           ST_ShiftLongitude(ST_Transform(ST_ClosestPoint(way, ST_Centroid(way)), 4326)) as P,
           tags
    from osm.planet_osm_line
    where highway in (\'motorway\',\'trunk\',\'primary\',\'secondary\',\'tertiary\',\'tertiary_link\',\'unclassified\',\'living_street\',\'residential\',\'motorway_link\',\'trunk_link\',\'primary_link\',\'secondary_link\')
      and ST_Intersects(way, ST_Transform(ST_SetSRID(ST_MakeBox2D(
                                                             ST_MakePoint($' . (count($SQL_Params) + 1) . '::NUMERIC, $' . (count($SQL_Params) + 2) . '::NUMERIC),
                                                             ST_MakePoint($' . (count($SQL_Params) + 3) . '::NUMERIC, $' . (count($SQL_Params) + 4) . '::NUMERIC)),
                                                     4674), 3857))
)
select jsonb_build_object(
               \'mode\', 3,
               \'dicts\',
               jsonb_build_object(\'dict1\',
                                  json_build_array(array(select distinct tags -> \'lit\' from T ORDER BY 1)) -> 0)::JSONB,
               \'geoms\', jsonb_build_object(
                       \'type\', \'FeatureCollection\',
                       \'features\', jsonb_build_array(array(
                        select jsonb_build_object(
                                       \'type\', \'Feature\',
                                       \'geometry\', ST_AsGeoJSON(ST_ShiftLongitude(ST_Transform(T.way, 4326)), 6)::JSONB,
                                       \'properties\',
                                       jsonb_build_object(
                                               \'l\', T.tags -> \'lit\',
                                               \'p_lat\', ST_Y(P)::NUMERIC,
                                               \'p_lng\', ST_X(P)::NUMERIC,
                                               \'pp\', ST_AsGeoJson(ST_ShiftLongitude(ST_Transform(st_union(array_agg(P.way)), 4326)), 6)
                                           )::JSONB
                                   )
                        from T T
                                 left join osm.planet_osm_point p
                                           on p.highway = \'street_lamp\' and ST_DWithin(T.way, P.way, 10)
                        group by T.WAY, T.TAGS, T.P
                    )) -> 0)::JSONB
           )::JSONB;';

			$SQL_Params[] = $bounds[1];
			$SQL_Params[] = $bounds[0];
			$SQL_Params[] = $bounds[3];
			$SQL_Params[] = $bounds[2];

		}else{
			Error_Answer('Обязательно указывать зону ограничения в запросе');
		}
		break;

	case 4: //Переходы
		if(count($bounds) > 0){
			$SQL = '
WITH T(way, P, tags) as (
    select ST_ShiftLongitude(ST_Transform(way, 4326))                                    as way,
           ST_ShiftLongitude(ST_Transform(ST_ClosestPoint(way, ST_Centroid(way)), 4326)) as P,
           tags
    from osm.planet_osm_line
    where highway = \'crossing\'
      and ST_Intersects(way, ST_Transform(ST_SetSRID(ST_MakeBox2D(
                                                             ST_MakePoint($' . (count($SQL_Params) + 1) . '::NUMERIC, $' . (count($SQL_Params) + 2) . '::NUMERIC),
                                                             ST_MakePoint($' . (count($SQL_Params) + 3) . '::NUMERIC, $' . (count($SQL_Params) + 4) . '::NUMERIC)),
                                                     4674), 3857))
    union all
    
    select ST_ShiftLongitude(ST_Transform(P.way, 4326)),
           ST_ShiftLongitude(ST_Transform(P.way, 4326)),
           P.tags
    from osm.planet_osm_point P
    where P.highway = \'crossing\'
      and ST_Intersects(way, ST_Transform(ST_SetSRID(ST_MakeBox2D(
                                                             ST_MakePoint($' . (count($SQL_Params) + 1) . '::NUMERIC, $' . (count($SQL_Params) + 2) . '::NUMERIC),
                                                             ST_MakePoint($' . (count($SQL_Params) + 3) . '::NUMERIC, $' . (count($SQL_Params) + 4) . '::NUMERIC)),
                                                     4674), 3857))
)
select jsonb_build_object(
               \'mode\', 4,
               \'dicts\', jsonb_build_object(\'dict1\', json_build_array(array(select distinct tags -> \'crossing\' from T ORDER BY 1)) -> 0)::JSONB,
               \'geoms\', jsonb_build_object(
                       \'type\', \'FeatureCollection\',
                       \'features\', jsonb_build_array(array(
                        select jsonb_build_object(
                                       \'type\', \'Feature\',
                                       \'geometry\', ST_AsGeoJSON(T.way, 6)::JSONB,
                                       \'properties\',
                                       jsonb_build_object(
                                               \'l\', T.tags -> \'crossing\',
                                               \'p_lat\', ST_Y(P)::NUMERIC,
                                               \'p_lng\', ST_X(P)::NUMERIC
                                           )::JSONB
                                   )
                        from T T
                    )) -> 0)::JSONB
           )::JSONB;';

			$SQL_Params[] = $bounds[1];
			$SQL_Params[] = $bounds[0];
			$SQL_Params[] = $bounds[3];
			$SQL_Params[] = $bounds[2];

		}else{
			Error_Answer('Обязательно указывать зону ограничения в запросе');
		}
		break;

	case 5:
		$osm_id = getIntParamByName('osm_id');
		if(!is_nan($osm_id)){
			$SQL = 'select ST_AsGeoJSON(ST_ShiftLongitude(ST_Transform(ST_Envelope(ST_Union(array_agg(way))), 4326)), 6) from osm.planet_osm_polygon where osm_id=$' . (count($SQL_Params) + 1) . ';';
			$SQL_Params[] = getIntParamByName('osm_id');

		}else{
			$town_name = getStrParamByName('town');
			if($town_name){
				$SQL = 'select ST_AsGeoJSON(ST_ShiftLongitude(ST_Transform(ST_Envelope(ST_Union(array_agg(way))), 4326)), 6) from osm.planet_osm_polygon where place in (\'town\', \'city\') and UPPER(name)=$' . (count($SQL_Params) + 1) . ';';
				$SQL_Params[] = strtoupper($town_name);
			}
		}
		break;

	default:
		Error_Answer('Неизвестный режим');
		break;
}

if($SQL !== ''){
	$conn = _openConnect();
	$query = pg_query_params($conn, $SQL, $SQL_Params);
	if($query){
		$data = null;
		$row = pg_fetch_row($query);
		if($row){
			$data = json_decode($row[0], false);
		}

		pg_free_result($query);

		Success_Answer($data);

	}else{
		Error_SQL_Answer($SQL, $SQL_Params, $conn, $query);
	}

	pg_free_result($query);

	_closeConnect($conn);
}