----------------------------------------------------

-- Создаём табличку с местами (административные границы) из OSM
drop table if exists places;
create table places as
select osm_id, (array_agg(name))[1] as name, (array_agg(admin_level))[1]::smallint as admin_level, st_union(array_agg(way)) as way
from planet_osm_polygon
where boundary = 'administrative'
group by osm_id;
grant all on places to public;
alter table places add constraint places_pk primary key (osm_id);
create index idx_places_admin_level on places (admin_level);
create index idx_places_way on places using gist (way);
analyse places;

-- Табличка с заданиями на скачивание DIFF файлов
create table diff
(
	n1 smallint not null,
	n2 smallint not null,
	state boolean default false not null,
	t timestamp default now() not null,
	work numeric,
	constraint diff_pk primary key (n1, n2)
);
grant all on diff to public;
insert into diff(n1, n2)
select n1, n2 from generate_series(695, 800) n1, generate_series(0, 999) n2 on conflict do nothing;
grant all on diff to public;

-- Таблица для складирования данных из DIFF файлов
drop table if exists history;
create table history
(
	mode      text,
	n_name    text    not null,
	id        bigint  not null,
	version   integer not null,
	timestamp timestamp,
	uid       bigint,
	osm_user  text,
	changeset bigint,
	lat       numeric,
	lon       numeric,
	tags      hstore,
	nodes     bigint[],
	members   jsonb,
	constraint history_pk primary key (id, version, n_name)
);
grant all on history to public;
create index idx_history_timestamp on history (timestamp);

----------------------------------------------------

-- Создаём таблицу для расчётов (она как копия таблицы с историей, но с индексами и прочими дополнениями)
drop table if exists hist;
create table hist
(
	mode text,
	n_name text not null,
	id bigint not null,
	version integer not null,
	timestamp timestamp,
	uid bigint,
	osm_user text,
	changeset bigint,
	lat numeric,
	lon numeric,
	tags hstore,
	nodes bigint[],
	members jsonb,
	point geometry,
	point_is_set bool default false,
	constraint hist_pk primary key (id, version, n_name)
);
create index idx_hist_id on hist (id);
create index idx_hist_uid on hist (uid);
create index idx_hist_id_version on hist (id);
create index idx_hist_id_user on hist (osm_user);
create index idx_hist_id_tags on hist using gin (tags);
create index idx_hist_id_members on hist using gin (members);
create index idx_hist_id_point on hist using gist (point);
create index idx_hist_id_timestamp on hist (timestamp);
create index idx_hist_id_point_is_set on hist (point_is_set);
grant all on hist to public;

analyse hist;

-------------------------------------------------

--Переносим последние дозагруженные данные
insert into hist select * from history where timestamp > (select max(timestamp) from hist) on conflict do nothing;

analyse hist;

-- Обновляем данные по точкам. То что можно, берётся из Диффов но на дату, не старше даты изменения, но при этом "последнюю" в этом наборе
-- Те точки, что не найдены в диффах, берутся из залитого дампа. Тут уже без версионности. Что есть, то есть
-- НЕ найденные вообще точки (у нас дамп только по России, а диффы по всему миру) просто игнорируются. Они нас, по сути, не интересуют
update hist
set point = st_transform(st_setSRID(st_makepoint(
		                                    case
			                                    when lon < -179.999999 then -179.999999
			                                    when lon > 179.999999 then 179.999999
			                                    else lon end,
		                                    case
			                                    when lat < -89.999999 then -89.999999
			                                    when lat > 89.999999 then 89.999999
			                                    else lat end
	                                    ), 4326), 3857),
    point_is_set = true
where lat is not null
  and lon is not null
  and point is null
  and point_is_set = false;

update hist h
set point = st_union(array(
		select p
		from (
			     select distinct h1.id,
			                     h1.version,
			                     h1.n,
			                     coalesce(
							                     last_value(h2.point) over (order by h2.version),
							                     (select st_transform(st_setsrid(st_makepoint(
									                                                     case
										                                                     when N.lon < -179.999999 then -179.999999
										                                                     when N.lon > 179.999999 then 179.999999
										                                                     else N.lon end,
									                                                     case
										                                                     when N.lat < -89.999999 then -89.999999
										                                                     when N.lat > 89.999999 then 89.999999
										                                                     else N.lat end
								                                                     ), 4326
								                                          ), 3857)
							                      from planet_osm_nodes n
							                      where n.id = h1.n)
				                     ) as p
			     from (
				          select distinct h.id, h.version, h.timestamp, unnest(h.nodes) as n
			          ) h1
				          left join hist h2
				                    on h1.timestamp <= h2.timestamp and h1.n = h2.id
		     ) T
		where P is not null
	)),
    point_is_set = true
where point is null
  and n_name = 'way'
  and point_is_set = false;

update hist h
set point = st_union(array(
		select p
		from (
			     select distinct t.ref as id,
			                     coalesce(
							                     last_value(h2.point) over (order by h2.version),
							                     (select st_transform(st_setsrid(st_makepoint(
									                                                     case
										                                                     when N.lon < -179.999999 then -179.999999
										                                                     when N.lon > 179.999999 then 179.999999
										                                                     else N.lon end,
									                                                     case
										                                                     when N.lat < -89.999999 then -89.999999
										                                                     when N.lat > 89.999999 then 89.999999
										                                                     else N.lat end
								                                                     ), 4326
								                                          ), 3857)
							                      from planet_osm_nodes n
							                      where n.id = t.ref)
				                     ) as p
			     from (select (jsonb_array_elements(h.members) ->> 'ref')::bigint) T(ref)
				          left join hist h2
				                    on h.timestamp <= h2.timestamp and t.ref = h2.id
		     ) T
		where P is not null
	))
where n_name = 'relation'
  and point is null
  and point_is_set = false;

analyse hist;

-----------------------------------------------------
-- Последняя метка для отчёта
select max(timestamp) from hist;

--------------------------------------
-- 0я страничка
select T3.osm_user,
       T3.mode,
       sum(T3.road_chng)            as road_chng,
       sum(T3.bus_stop_chng)        as bus_stop_chng,
       sum(T3.hw_bus_stop_chng)     as hw_bus_stop_chng,
       sum(T3.lines_chng)           as lines_chng,
       sum(T3.lit_chng)             as lit_chng,
       sum(T3.maxspeed_chng)        as maxspeed_chng,
       sum(T3.crossing_island_chng) as crossing_island_chng,
       sum(T3.traffic_calming_chng) as traffic_calming_chng,
       sum(T3.crossing_chng)        as crossing_chng
from (
	     -- Получаем суммы изменений в разрезе участник/день по геометриям.
	     select osm_user,
	            mode,
	            sum(road_chng)            as road_chng,
	            sum(bus_stop_chng)        as bus_stop_chng,
	            sum(hw_bus_stop_chng)     as hw_bus_stop_chng,
	            sum(lines_chng)           as lines_chng,
	            sum(lit_chng)             as lit_chng,
	            sum(maxspeed_chng)        as maxspeed_chng,
	            sum(crossing_island_chng) as crossing_island_chng,
	            sum(traffic_calming_chng) as traffic_calming_chng,
	            sum(crossing_chng)        as crossing_chng
	     from (
		          --Из первички выделяем изменения по интересующим нас тегам и высотавляем соответствующий флаг
		          select id,
		                 version,
		                 mode,
		                 osm_user,
		                 case
			                 when roads is not null and
			                      (roads <> lag(roads) over (partition by id order by version) or lag(roads) over (partition by id order by version) is null)
				                 then 1
			                 else 0 end as road_chng,
		                 case
			                 when bus_stop is not null and
			                      (bus_stop <> lag(bus_stop) over (partition by id order by version) or lag(bus_stop) over (partition by id order by version) is null)
				                 then 1
			                 else 0 end as bus_stop_chng,
		                 case
			                 when hw_bus_stop is not null and
			                      (hw_bus_stop <> lag(hw_bus_stop) over (partition by id order by version) or lag(hw_bus_stop) over (partition by id order by version) is null)
				                 then 1
			                 else 0 end as hw_bus_stop_chng,
		                 case
			                 when lines is not null and
			                      (lines <> lag(lines) over (partition by id order by version) or lag(lines) over (partition by id order by version) is null)
				                 then 1
			                 else 0 end as lines_chng,
		                 case
			                 when lit is not null and
			                      (lit <> lag(lit) over (partition by id order by version) or lag(lit) over (partition by id order by version) is null)
				                 then 1
			                 else 0 end as lit_chng,
		                 case
			                 when maxspeed is not null and
			                      (maxspeed <> lag(maxspeed) over (partition by id order by version) or lag(maxspeed) over (partition by id order by version) is null)
				                 then 1
			                 else 0 end as maxspeed_chng,
		                 case
			                 when crossing_island is not null and
			                      (crossing_island <> lag(crossing_island) over (partition by id order by version) or lag(crossing_island) over (partition by id order by version) is null)
				                 then 1
			                 else 0 end as crossing_island_chng,
		                 case
			                 when traffic_calming is not null and
			                      (traffic_calming <> lag(traffic_calming) over (partition by id order by version) or lag(traffic_calming) over (partition by id order by version) is null)
				                 then 1
			                 else 0 end as traffic_calming_chng,
		                 case
			                 when crossing is not null and
			                      (crossing <> lag(crossing) over (partition by id order by version) or lag(crossing) over (partition by id order by version) is null)
				                 then 1
			                 else 0 end as crossing_chng
		          from (
			               -- Первичка. Оббираем только те изменения, которые находятся на территории РФ и только с даты старта конкурса
			               select osm_user,
			                      id,
			                      version,
			                      mode,
			                      case
				                      when tags -> 'highway' = any (ARRAY ['motorway', 'trunk', 'primary', 'secondary', 'tertiary', 'tertiary_link', 'unclassified',
					                      'living_street', 'residential', 'motorway_link', 'trunk_link', 'primary_link', 'secondary_link', 'service']::TEXT[]) then tags -> 'highway'
				                      else null end                                                              as roads,
			                      case when tags -> 'highway' = 'bus_stop' then tags -> 'highway' else null end  as bus_stop,
			                      case when tags -> 'crossing' = 'bus_stop' then tags -> 'highway' else null end as hw_bus_stop,
			                      tags -> 'lines'                                                                as lines,
			                      tags -> 'lit'                                                                  as lit,
			                      tags -> 'maxspeed'                                                             as maxspeed,
			                      tags -> 'crossing:island'                                                      as crossing_island,
			                      tags -> 'traffic_calming'                                                      as traffic_calming,
			                      tags -> 'crossing'                                                             as crossing
			               from hist
			               where (tags ? 'highway'
				               or tags ? 'lines'
				               or tags ? 'lit'
				               or tags ? 'maxspeed'
				               or tags ? 'crossing:island'
				               or tags ? 'traffic_calming'
				               or tags ? 'crossing')
				             and st_within(point, (select way from places where admin_level = 2 limit 1))
				             and timestamp >= '10.09.2021'::TIMESTAMP
		               ) T1
	          ) T2
	     where osm_user = ANY (array [
		     'OSM User','Kachkaev','fndoder','New-Yurok','Timur Gilfanov','evgenykatyshev','ceekay80','Shoorick','Uralla','Viacheslav Gavrilov','Антон007','AlexanderZuevVlg','ks1v',
	         'Alexey Sirotkin','George897','vgrekhov','sikmir','Никитин Николай','AleksandrB007','literan','UrbanGleb','mishaulitskiy','ВаленВ','weary_cynic','lord_Alpaca','pudov_maxim',
	         'n-klyshko','Глеб','borkabritva','AleksandrB007','Васильев Андрей','SoNick_RND','dom159','pfg21','Ignatych','Shortparis','fofanovamv','Shkoda_Ula','AsySmile','Daniil M',
	         'Calmarina ','ZinaidaZ','polkadastr','Vs_80','Dunaich','Corsa5','Заур Гитинов','Айнур Мифтахов','Polina Korolchuk','Екатерина Леттиева','Александр Камашев','nonameforme',
	         'toxat','Nika_no','Evgeniy Starobinets','Allin_kindA','Андрей_Горшков','Фред-Продавец звёзд','4monstrik ','Vs_80','Alex94','flexagoon','Qwertin','annsk29','yorik_iv',
	         'Сержикulsk','Vanchelo','Sanchos159','ЭндрюГарфилд','VRSS','Леонид1997','Olga_E','tuhgus','Som','Aluxantis','mttanya','Anatoliy Prav','messof_anegovich','Sophie_Framboise',
	         'Roma_Boyko','byBAC','ortieom','gulya_akh','polich','classistrip','MadFatVlad','konovalova2207','IliaKrest','TellauR','Luis_Giedroyc','shvedovskiy','ольга сновская','Morkou',
	         'RadmirSad','o4en_moryak','Аханов Дмитрий','Куртуков Константин','dbrwin','Васи Лий','kangu10','qut','sin(x)','pe_skin','panaramix','ra44o','rukus97','adonskoi','Flonger',
	         'GermanLip','ALeXiOZZZ','alexashh','DvinaLand','Валерий Зубанов','0ddo','CheySer','Jane Freud','ArsGaz','Nik Kras','nikitadnlnk','ItzVektor','Sadless74','beer_absorber',
	         'silverst0ne','Rybaso','p_cepheus','sawser','Дядя Ваня Saw','Zacky27','ksvl','Arrrriva','earnestmaps','Василий Александров','Alekzzander','az09','Sandrro','Марья Самородская',
	         '_COOLer_','Rudennn','simsanutiy','PashArt','sqopa','Daniil M','Ольга Коняева','voilashechkin','dmitryborzenkov','rasscrom','AnnMaps2019','alexcheln','Zema34','Augusto Pinochet',
	         'Халида1','Анна Алексеева ','lembit@bitrix24.ru','Ержанчик','VORON_SPb','dergilyova','epidzhx','Laperuza712','maslinych','ekaterina_tei','dezdichado','mira5o','Sipina anastasiya',
	         'нету','11daf','Аня Жаксыбаева','Alex_Boro','filippov70','Ser9ei','Ilya Dontsov','yudin_aa','Const@nt','Lichtenblaster','germangac','Loskir','mitya9697','Z_phyr','Ser9ei',
	         'Woislav','Ershovns','DmitriySaw','vlalexey','Nikolay Podoprigora','Ln13','yaKonovalov','MatveyBub','arina_kamm','Beshanian','pesec','nagor_ant','Artur Petrosian','Sysertchanin',
	         'МаринаZhucheva','j9j','Netdezhda','Георгий412','k7223451','Lazarevsk','rybkolub','Elemzal','petelya','doofyrocks','bravebug','vrodetema','prmkzn','TrickyFoxy','vnogetopor',
	         'Snaark','jmty8','serega1103','cupivan','DeKaN','YatsenkoAnton','Yury Kryvashei','Alexander Rublewski','Srojeco','sph_cow','fendme','Mapkbee','haskini','LeenHis','BCNorwich',
	         'Dzindevis','moonstar-kate','Alexey369i','NetJorika','Роман Платунов','alena_light28','d1sr4n','pacman541','d1sr4n','VlIvYur','Ranas','mrKPbIS','klrnv','Lina_Shpileva_GIS',
	         'dannyloumax','Владимир К','mshilova2003','wosk','Илюха2012','playerr17 ','ArtemVart','Laavang','posts2000','evgenia_osm','subjectnamehere','АлисаЛалиса','Belousov Aleksei',
	         'Kuri Mudro','VdmrO','Kron418','alexeybelyalov','alievahmed','sergsmx','kasimov.an@gmail.com','BusteR2712','MaksN','EkaterinaStepurko','Pashandy','Daria0101','Глеб Серебряный',
	         'Teirudeag','Дарья Брондзя','tanyalotsman','pgisinet','etelinda1986','b00','Alex77','FundaYury','vlllaza','alex_cherrie','2albert','не регистрируется','Полина Игнатенко','Skro11',
	         'Denis_Bakharev_98','alexander_mart','Троллейбус','deidpw','frolove','-off-','ivanbezin','Gorovaja','Arekasu','ValenVolg','полина шурупова','Матвей Гомон','fotohotkovo',
	         'Vasiliy Nazarov','EvgeniyV95','EvgeniyV95','BearDan','polinapeshko','Chingis0811','Kira Utred','Vvo82','an_gorb','nastiaboyne','Анастасия Кононова ','Raul_Tejada','AmritaDhali',
	         'Vlad_Suz','was303'
		     ]::text[])
	     group by id, osm_user, mode) T3
group by t3.osm_user, t3.mode;

--------------------------------------
-- 1я страничка
select t3.day,
       T3.osm_user,
       T3.mode,
       sum(T3.road_chng)            as road_chng,
       sum(T3.bus_stop_chng)        as bus_stop_chng,
       sum(T3.hw_bus_stop_chng)     as hw_bus_stop_chng,
       sum(T3.lines_chng)           as lines_chng,
       sum(T3.lit_chng)             as lit_chng,
       sum(T3.maxspeed_chng)        as maxspeed_chng,
       sum(T3.crossing_island_chng) as crossing_island_chng,
       sum(T3.traffic_calming_chng) as traffic_calming_chng,
       sum(T3.crossing_chng)        as crossing_chng
from (
	     select day,
	            osm_user,
	            mode,
	            sum(road_chng)            as road_chng,
	            sum(bus_stop_chng)        as bus_stop_chng,
	            sum(hw_bus_stop_chng)     as hw_bus_stop_chng,
	            sum(lines_chng)           as lines_chng,
	            sum(lit_chng)             as lit_chng,
	            sum(maxspeed_chng)        as maxspeed_chng,
	            sum(crossing_island_chng) as crossing_island_chng,
	            sum(traffic_calming_chng) as traffic_calming_chng,
	            sum(crossing_chng)        as crossing_chng
	     from (
		          select day,
		                 id,
		                 version,
		                 mode,
		                 n_name,
		                 osm_user,
		                 case
			                 when roads is not null and
			                      (roads <> lag(roads) over (partition by id order by version) or lag(roads) over (partition by id order by version) is null)
				                 then 1
			                 else 0 end as road_chng,
		                 case
			                 when bus_stop is not null and
			                      (bus_stop <> lag(bus_stop) over (partition by id order by version) or lag(bus_stop) over (partition by id order by version) is null)
				                 then 1
			                 else 0 end as bus_stop_chng,
		                 case
			                 when hw_bus_stop is not null and
			                      (hw_bus_stop <> lag(hw_bus_stop) over (partition by id order by version) or lag(hw_bus_stop) over (partition by id order by version) is null)
				                 then 1
			                 else 0 end as hw_bus_stop_chng,
		                 case
			                 when lines is not null and
			                      (lines <> lag(lines) over (partition by id order by version) or lag(lines) over (partition by id order by version) is null)
				                 then 1
			                 else 0 end as lines_chng,
		                 case
			                 when lit is not null and
			                      (lit <> lag(lit) over (partition by id order by version) or lag(lit) over (partition by id order by version) is null)
				                 then 1
			                 else 0 end as lit_chng,
		                 case
			                 when maxspeed is not null and
			                      (maxspeed <> lag(maxspeed) over (partition by id order by version) or lag(maxspeed) over (partition by id order by version) is null)
				                 then 1
			                 else 0 end as maxspeed_chng,
		                 case
			                 when crossing_island is not null and
			                      (crossing_island <> lag(crossing_island) over (partition by id order by version) or lag(crossing_island) over (partition by id order by version) is null)
				                 then 1
			                 else 0 end as crossing_island_chng,
		                 case
			                 when traffic_calming is not null and
			                      (traffic_calming <> lag(traffic_calming) over (partition by id order by version) or lag(traffic_calming) over (partition by id order by version) is null)
				                 then 1
			                 else 0 end as traffic_calming_chng,
		                 case
			                 when crossing is not null and
			                      (crossing <> lag(crossing) over (partition by id order by version) or lag(crossing) over (partition by id order by version) is null)
				                 then 1
			                 else 0 end as crossing_chng
		          from (
			               select osm_user,
			                      id,
			                      version,
			                      n_name,
			                      mode,
			                      to_char(timestamp, 'DD.MM')                                                    as day,
			                      case
				                      when tags -> 'highway' = any (ARRAY ['motorway', 'trunk', 'primary', 'secondary', 'tertiary', 'tertiary_link', 'unclassified',
					                      'living_street', 'residential', 'motorway_link', 'trunk_link', 'primary_link', 'secondary_link', 'service']::TEXT[]) then tags -> 'highway'
				                      else null end                                                              as roads,
			                      case when tags -> 'highway' = 'bus_stop' then tags -> 'highway' else null end  as bus_stop,
			                      case when tags -> 'crossing' = 'bus_stop' then tags -> 'highway' else null end as hw_bus_stop,
			                      tags -> 'lines'                                                                as lines,
			                      tags -> 'lit'                                                                  as lit,
			                      tags -> 'maxspeed'                                                             as maxspeed,
			                      tags -> 'crossing:island'                                                      as crossing_island,
			                      tags -> 'traffic_calming'                                                      as traffic_calming,
			                      tags -> 'crossing'                                                             as crossing
			               from hist
			               where (tags ? 'highway'
				               or tags ? 'lines'
				               or tags ? 'lit'
				               or tags ? 'maxspeed'
				               or tags ? 'crossing:island'
				               or tags ? 'traffic_calming'
				               or tags ? 'crossing')
				             and st_intersects(point, (select way from places where admin_level = 2 limit 1))
		               ) T1
	          ) T2
	     where osm_user = ANY (array [
		     'OSM User','Kachkaev','fndoder','New-Yurok','Timur Gilfanov','evgenykatyshev','ceekay80','Shoorick','Uralla','Viacheslav Gavrilov','Антон007','AlexanderZuevVlg','ks1v',
	         'Alexey Sirotkin','George897','vgrekhov','sikmir','Никитин Николай','AleksandrB007','literan','UrbanGleb','mishaulitskiy','ВаленВ','weary_cynic','lord_Alpaca','pudov_maxim',
	         'n-klyshko','Глеб','borkabritva','AleksandrB007','Васильев Андрей','SoNick_RND','dom159','pfg21','Ignatych','Shortparis','fofanovamv','Shkoda_Ula','AsySmile','Daniil M',
	         'Calmarina ','ZinaidaZ','polkadastr','Vs_80','Dunaich','Corsa5','Заур Гитинов','Айнур Мифтахов','Polina Korolchuk','Екатерина Леттиева','Александр Камашев','nonameforme',
	         'toxat','Nika_no','Evgeniy Starobinets','Allin_kindA','Андрей_Горшков','Фред-Продавец звёзд','4monstrik ','Vs_80','Alex94','flexagoon','Qwertin','annsk29','yorik_iv',
	         'Сержикulsk','Vanchelo','Sanchos159','ЭндрюГарфилд','VRSS','Леонид1997','Olga_E','tuhgus','Som','Aluxantis','mttanya','Anatoliy Prav','messof_anegovich','Sophie_Framboise',
	         'Roma_Boyko','byBAC','ortieom','gulya_akh','polich','classistrip','MadFatVlad','konovalova2207','IliaKrest','TellauR','Luis_Giedroyc','shvedovskiy','ольга сновская','Morkou',
	         'RadmirSad','o4en_moryak','Аханов Дмитрий','Куртуков Константин','dbrwin','Васи Лий','kangu10','qut','sin(x)','pe_skin','panaramix','ra44o','rukus97','adonskoi','Flonger',
	         'GermanLip','ALeXiOZZZ','alexashh','DvinaLand','Валерий Зубанов','0ddo','CheySer','Jane Freud','ArsGaz','Nik Kras','nikitadnlnk','ItzVektor','Sadless74','beer_absorber',
	         'silverst0ne','Rybaso','p_cepheus','sawser','Дядя Ваня Saw','Zacky27','ksvl','Arrrriva','earnestmaps','Василий Александров','Alekzzander','az09','Sandrro','Марья Самородская',
	         '_COOLer_','Rudennn','simsanutiy','PashArt','sqopa','Daniil M','Ольга Коняева','voilashechkin','dmitryborzenkov','rasscrom','AnnMaps2019','alexcheln','Zema34','Augusto Pinochet',
	         'Халида1','Анна Алексеева ','lembit@bitrix24.ru','Ержанчик','VORON_SPb','dergilyova','epidzhx','Laperuza712','maslinych','ekaterina_tei','dezdichado','mira5o','Sipina anastasiya',
	         'нету','11daf','Аня Жаксыбаева','Alex_Boro','filippov70','Ser9ei','Ilya Dontsov','yudin_aa','Const@nt','Lichtenblaster','germangac','Loskir','mitya9697','Z_phyr','Ser9ei',
	         'Woislav','Ershovns','DmitriySaw','vlalexey','Nikolay Podoprigora','Ln13','yaKonovalov','MatveyBub','arina_kamm','Beshanian','pesec','nagor_ant','Artur Petrosian','Sysertchanin',
	         'МаринаZhucheva','j9j','Netdezhda','Георгий412','k7223451','Lazarevsk','rybkolub','Elemzal','petelya','doofyrocks','bravebug','vrodetema','prmkzn','TrickyFoxy','vnogetopor',
	         'Snaark','jmty8','serega1103','cupivan','DeKaN','YatsenkoAnton','Yury Kryvashei','Alexander Rublewski','Srojeco','sph_cow','fendme','Mapkbee','haskini','LeenHis','BCNorwich',
	         'Dzindevis','moonstar-kate','Alexey369i','NetJorika','Роман Платунов','alena_light28','d1sr4n','pacman541','d1sr4n','VlIvYur','Ranas','mrKPbIS','klrnv','Lina_Shpileva_GIS',
	         'dannyloumax','Владимир К','mshilova2003','wosk','Илюха2012','playerr17 ','ArtemVart','Laavang','posts2000','evgenia_osm','subjectnamehere','АлисаЛалиса','Belousov Aleksei',
	         'Kuri Mudro','VdmrO','Kron418','alexeybelyalov','alievahmed','sergsmx','kasimov.an@gmail.com','BusteR2712','MaksN','EkaterinaStepurko','Pashandy','Daria0101','Глеб Серебряный',
	         'Teirudeag','Дарья Брондзя','tanyalotsman','pgisinet','etelinda1986','b00','Alex77','FundaYury','vlllaza','alex_cherrie','2albert','не регистрируется','Полина Игнатенко','Skro11',
	         'Denis_Bakharev_98','alexander_mart','Троллейбус','deidpw','frolove','-off-','ivanbezin','Gorovaja','Arekasu','ValenVolg','полина шурупова','Матвей Гомон','fotohotkovo',
	         'Vasiliy Nazarov','EvgeniyV95','EvgeniyV95','BearDan','polinapeshko','Chingis0811','Kira Utred','Vvo82','an_gorb','nastiaboyne','Анастасия Кононова ','Raul_Tejada','AmritaDhali',
	         'Vlad_Suz','was303'
		     ]::text[])
	     group by id, n_name, day, osm_user, mode) T3
group by t3.day, t3.osm_user, t3.mode;

--------------------------------------
-- 2я страничка
select t.name,
       t3.day,
       T3.mode,
       array_sort_unique(array_agg(osm_user)) as users,
       sum(T3.road_chng)                      as road_chng,
       sum(T3.bus_stop_chng)                  as bus_stop_chng,
       sum(T3.hw_bus_stop_chng)               as hw_bus_stop_chng,
       sum(T3.lines_chng)                     as lines_chng,
       sum(T3.lit_chng)                       as lit_chng,
       sum(T3.maxspeed_chng)                  as maxspeed_chng,
       sum(T3.crossing_island_chng)           as crossing_island_chng,
       sum(T3.traffic_calming_chng)           as traffic_calming_chng,
       sum(T3.crossing_chng)                  as crossing_chng
from (
	     select id,
	            n_name,
	            day,
	            osm_user,
	            mode,
	            max(version)               as version,
	            array_agg(roads)           as roads,
	            sum(road_chng)             as road_chng,
	            array_agg(bus_stop)        as bus_stop,
	            sum(bus_stop_chng)         as bus_stop_chng,
	            array_agg(hw_bus_stop)     as hw_bus_stop,
	            sum(hw_bus_stop_chng)      as hw_bus_stop_chng,
	            array_agg(lines)           as lines,
	            sum(lines_chng)            as lines_chng,
	            array_agg(lit)             as lit,
	            sum(lit_chng)              as lit_chng,
	            array_agg(maxspeed)        as maxspeed,
	            sum(maxspeed_chng)         as maxspeed_chng,
	            array_agg(crossing_island) as crossing_island,
	            sum(crossing_island_chng)  as crossing_island_chng,
	            array_agg(traffic_calming) as traffic_calming,
	            sum(traffic_calming_chng)  as traffic_calming_chng,
	            array_agg(crossing)        as crossing,
	            sum(crossing_chng)         as crossing_chng,
	            st_union(array_agg(point))    points
	     from (
		          select day,
		                 id,
		                 version,
		                 mode,
		                 n_name,
		                 osm_user,
		                 roads           as roads,
		                 case
			                 when roads is not null and
			                      (roads <> lag(roads) over (partition by id order by version) or lag(roads) over (partition by id order by version) is null)
				                 then 1
			                 else 0 end  as road_chng,
		                 bus_stop        as bus_stop,
		                 case
			                 when bus_stop is not null and
			                      (bus_stop <> lag(bus_stop) over (partition by id order by version) or lag(bus_stop) over (partition by id order by version) is null)
				                 then 1
			                 else 0 end  as bus_stop_chng,
		                 hw_bus_stop     as hw_bus_stop,
		                 case
			                 when hw_bus_stop is not null and
			                      (hw_bus_stop <> lag(hw_bus_stop) over (partition by id order by version) or lag(hw_bus_stop) over (partition by id order by version) is null)
				                 then 1
			                 else 0 end  as hw_bus_stop_chng,
		                 lines           as lines,
		                 case
			                 when lines is not null and
			                      (lines <> lag(lines) over (partition by id order by version) or lag(lines) over (partition by id order by version) is null)
				                 then 1
			                 else 0 end  as lines_chng,
		                 lit             as lit,
		                 case
			                 when lit is not null and
			                      (lit <> lag(lit) over (partition by id order by version) or lag(lit) over (partition by id order by version) is null)
				                 then 1
			                 else 0 end  as lit_chng,
		                 maxspeed        as maxspeed,
		                 case
			                 when maxspeed is not null and
			                      (maxspeed <> lag(maxspeed) over (partition by id order by version) or lag(maxspeed) over (partition by id order by version) is null)
				                 then 1
			                 else 0 end  as maxspeed_chng,
		                 crossing_island as crossing_island,
		                 case
			                 when crossing_island is not null and
			                      (crossing_island <> lag(crossing_island) over (partition by id order by version) or lag(crossing_island) over (partition by id order by version) is null)
				                 then 1
			                 else 0 end  as crossing_island_chng,
		                 traffic_calming as traffic_calming,
		                 case
			                 when traffic_calming is not null and
			                      (traffic_calming <> lag(traffic_calming) over (partition by id order by version) or lag(traffic_calming) over (partition by id order by version) is null)
				                 then 1
			                 else 0 end  as traffic_calming_chng,
		                 crossing        as crossing,
		                 case
			                 when crossing is not null and
			                      (crossing <> lag(crossing) over (partition by id order by version) or lag(crossing) over (partition by id order by version) is null)
				                 then 1
			                 else 0 end  as crossing_chng,
		                 point
		          from (
			               select osm_user,
			                      id,
			                      version,
			                      n_name,
			                      mode,
			                      to_char(timestamp, 'DD.MM')                                                    as day,
			                      case
				                      when tags -> 'highway' = any (ARRAY ['motorway', 'trunk', 'primary', 'secondary', 'tertiary', 'tertiary_link', 'unclassified',
					                      'living_street', 'residential', 'motorway_link', 'trunk_link', 'primary_link', 'secondary_link', 'service']::TEXT[]) then tags -> 'highway'
				                      else null end                                                              as roads,
			                      case when tags -> 'highway' = 'bus_stop' then tags -> 'highway' else null end  as bus_stop,
			                      case when tags -> 'crossing' = 'bus_stop' then tags -> 'highway' else null end as hw_bus_stop,
			                      tags -> 'lines'                                                                as lines,
			                      tags -> 'lit'                                                                  as lit,
			                      tags -> 'maxspeed'                                                             as maxspeed,
			                      tags -> 'crossing:island'                                                      as crossing_island,
			                      tags -> 'traffic_calming'                                                      as traffic_calming,
			                      tags -> 'crossing'                                                             as crossing,
			                      point
			               from hist
			               where (tags ? 'highway'
				               or tags ? 'lines'
				               or tags ? 'lit'
				               or tags ? 'maxspeed'
				               or tags ? 'crossing:island'
				               or tags ? 'traffic_calming'
				               or tags ? 'crossing')
				             and st_intersects(point, (select way from places where admin_level = 2 limit 1))
		               ) T1
	          ) T2
	     where osm_user = ANY (array [
		     'OSM User','Kachkaev','fndoder','New-Yurok','Timur Gilfanov','evgenykatyshev','ceekay80','Shoorick','Uralla','Viacheslav Gavrilov','Антон007','AlexanderZuevVlg','ks1v',
	         'Alexey Sirotkin','George897','vgrekhov','sikmir','Никитин Николай','AleksandrB007','literan','UrbanGleb','mishaulitskiy','ВаленВ','weary_cynic','lord_Alpaca','pudov_maxim',
	         'n-klyshko','Глеб','borkabritva','AleksandrB007','Васильев Андрей','SoNick_RND','dom159','pfg21','Ignatych','Shortparis','fofanovamv','Shkoda_Ula','AsySmile','Daniil M',
	         'Calmarina ','ZinaidaZ','polkadastr','Vs_80','Dunaich','Corsa5','Заур Гитинов','Айнур Мифтахов','Polina Korolchuk','Екатерина Леттиева','Александр Камашев','nonameforme',
	         'toxat','Nika_no','Evgeniy Starobinets','Allin_kindA','Андрей_Горшков','Фред-Продавец звёзд','4monstrik ','Vs_80','Alex94','flexagoon','Qwertin','annsk29','yorik_iv',
	         'Сержикulsk','Vanchelo','Sanchos159','ЭндрюГарфилд','VRSS','Леонид1997','Olga_E','tuhgus','Som','Aluxantis','mttanya','Anatoliy Prav','messof_anegovich','Sophie_Framboise',
	         'Roma_Boyko','byBAC','ortieom','gulya_akh','polich','classistrip','MadFatVlad','konovalova2207','IliaKrest','TellauR','Luis_Giedroyc','shvedovskiy','ольга сновская','Morkou',
	         'RadmirSad','o4en_moryak','Аханов Дмитрий','Куртуков Константин','dbrwin','Васи Лий','kangu10','qut','sin(x)','pe_skin','panaramix','ra44o','rukus97','adonskoi','Flonger',
	         'GermanLip','ALeXiOZZZ','alexashh','DvinaLand','Валерий Зубанов','0ddo','CheySer','Jane Freud','ArsGaz','Nik Kras','nikitadnlnk','ItzVektor','Sadless74','beer_absorber',
	         'silverst0ne','Rybaso','p_cepheus','sawser','Дядя Ваня Saw','Zacky27','ksvl','Arrrriva','earnestmaps','Василий Александров','Alekzzander','az09','Sandrro','Марья Самородская',
	         '_COOLer_','Rudennn','simsanutiy','PashArt','sqopa','Daniil M','Ольга Коняева','voilashechkin','dmitryborzenkov','rasscrom','AnnMaps2019','alexcheln','Zema34','Augusto Pinochet',
	         'Халида1','Анна Алексеева ','lembit@bitrix24.ru','Ержанчик','VORON_SPb','dergilyova','epidzhx','Laperuza712','maslinych','ekaterina_tei','dezdichado','mira5o','Sipina anastasiya',
	         'нету','11daf','Аня Жаксыбаева','Alex_Boro','filippov70','Ser9ei','Ilya Dontsov','yudin_aa','Const@nt','Lichtenblaster','germangac','Loskir','mitya9697','Z_phyr','Ser9ei',
	         'Woislav','Ershovns','DmitriySaw','vlalexey','Nikolay Podoprigora','Ln13','yaKonovalov','MatveyBub','arina_kamm','Beshanian','pesec','nagor_ant','Artur Petrosian','Sysertchanin',
	         'МаринаZhucheva','j9j','Netdezhda','Георгий412','k7223451','Lazarevsk','rybkolub','Elemzal','petelya','doofyrocks','bravebug','vrodetema','prmkzn','TrickyFoxy','vnogetopor',
	         'Snaark','jmty8','serega1103','cupivan','DeKaN','YatsenkoAnton','Yury Kryvashei','Alexander Rublewski','Srojeco','sph_cow','fendme','Mapkbee','haskini','LeenHis','BCNorwich',
	         'Dzindevis','moonstar-kate','Alexey369i','NetJorika','Роман Платунов','alena_light28','d1sr4n','pacman541','d1sr4n','VlIvYur','Ranas','mrKPbIS','klrnv','Lina_Shpileva_GIS',
	         'dannyloumax','Владимир К','mshilova2003','wosk','Илюха2012','playerr17 ','ArtemVart','Laavang','posts2000','evgenia_osm','subjectnamehere','АлисаЛалиса','Belousov Aleksei',
	         'Kuri Mudro','VdmrO','Kron418','alexeybelyalov','alievahmed','sergsmx','kasimov.an@gmail.com','BusteR2712','MaksN','EkaterinaStepurko','Pashandy','Daria0101','Глеб Серебряный',
	         'Teirudeag','Дарья Брондзя','tanyalotsman','pgisinet','etelinda1986','b00','Alex77','FundaYury','vlllaza','alex_cherrie','2albert','не регистрируется','Полина Игнатенко','Skro11',
	         'Denis_Bakharev_98','alexander_mart','Троллейбус','deidpw','frolove','-off-','ivanbezin','Gorovaja','Arekasu','ValenVolg','полина шурупова','Матвей Гомон','fotohotkovo',
	         'Vasiliy Nazarov','EvgeniyV95','EvgeniyV95','BearDan','polinapeshko','Chingis0811','Kira Utred','Vvo82','an_gorb','nastiaboyne','Анастасия Кононова ','Raul_Tejada','AmritaDhali',
	         'Vlad_Suz','was303'
		     ]::text[])
	     group by id, n_name, day, osm_user, mode) T3
	     join towns t
	          on st_intersects(points, t.way)
group by t.name, t3.day, t3.mode;

--------------------------------------
-- 3я страничка
select t3.day,
       T3.mode,
       sum(T3.road_chng)            as road_chng,
       sum(T3.bus_stop_chng)        as bus_stop_chng,
       sum(T3.hw_bus_stop_chng)     as hw_bus_stop_chng,
       sum(T3.lines_chng)           as lines_chng,
       sum(T3.lit_chng)             as lit_chng,
       sum(T3.maxspeed_chng)        as maxspeed_chng,
       sum(T3.crossing_island_chng) as crossing_island_chng,
       sum(T3.traffic_calming_chng) as traffic_calming_chng,
       sum(T3.crossing_chng)        as crossing_chng
from (
	     select day,
	            mode,
	            sum(road_chng)            as road_chng,
	            sum(bus_stop_chng)        as bus_stop_chng,
	            sum(hw_bus_stop_chng)     as hw_bus_stop_chng,
	            sum(lines_chng)           as lines_chng,
	            sum(lit_chng)             as lit_chng,
	            sum(maxspeed_chng)        as maxspeed_chng,
	            sum(crossing_island_chng) as crossing_island_chng,
	            sum(traffic_calming_chng) as traffic_calming_chng,
	            sum(crossing_chng)        as crossing_chng
	     from (
		          select day,
		                 id,
		                 version,
		                 mode,
		                 n_name,
		                 osm_user,
		                 case
			                 when roads is not null and
			                      (roads <> lag(roads) over (partition by id order by version) or lag(roads) over (partition by id order by version) is null)
				                 then 1
			                 else 0 end as road_chng,
		                 case
			                 when bus_stop is not null and
			                      (bus_stop <> lag(bus_stop) over (partition by id order by version) or lag(bus_stop) over (partition by id order by version) is null)
				                 then 1
			                 else 0 end as bus_stop_chng,
		                 case
			                 when hw_bus_stop is not null and
			                      (hw_bus_stop <> lag(hw_bus_stop) over (partition by id order by version) or lag(hw_bus_stop) over (partition by id order by version) is null)
				                 then 1
			                 else 0 end as hw_bus_stop_chng,
		                 case
			                 when lines is not null and
			                      (lines <> lag(lines) over (partition by id order by version) or lag(lines) over (partition by id order by version) is null)
				                 then 1
			                 else 0 end as lines_chng,
		                 case
			                 when lit is not null and
			                      (lit <> lag(lit) over (partition by id order by version) or lag(lit) over (partition by id order by version) is null)
				                 then 1
			                 else 0 end as lit_chng,
		                 case
			                 when maxspeed is not null and
			                      (maxspeed <> lag(maxspeed) over (partition by id order by version) or lag(maxspeed) over (partition by id order by version) is null)
				                 then 1
			                 else 0 end as maxspeed_chng,
		                 case
			                 when crossing_island is not null and
			                      (crossing_island <> lag(crossing_island) over (partition by id order by version) or lag(crossing_island) over (partition by id order by version) is null)
				                 then 1
			                 else 0 end as crossing_island_chng,
		                 case
			                 when traffic_calming is not null and
			                      (traffic_calming <> lag(traffic_calming) over (partition by id order by version) or lag(traffic_calming) over (partition by id order by version) is null)
				                 then 1
			                 else 0 end as traffic_calming_chng,
		                 case
			                 when crossing is not null and
			                      (crossing <> lag(crossing) over (partition by id order by version) or lag(crossing) over (partition by id order by version) is null)
				                 then 1
			                 else 0 end as crossing_chng
		          from (
			               select osm_user,
			                      id,
			                      version,
			                      n_name,
			                      mode,
			                      to_char(timestamp, 'DD.MM')                                                    as day,
			                      case
				                      when tags -> 'highway' = any (ARRAY ['motorway', 'trunk', 'primary', 'secondary', 'tertiary', 'tertiary_link', 'unclassified',
					                      'living_street', 'residential', 'motorway_link', 'trunk_link', 'primary_link', 'secondary_link', 'service']::TEXT[]) then tags -> 'highway'
				                      else null end                                                              as roads,
			                      case when tags -> 'highway' = 'bus_stop' then tags -> 'highway' else null end  as bus_stop,
			                      case when tags -> 'crossing' = 'bus_stop' then tags -> 'highway' else null end as hw_bus_stop,
			                      tags -> 'lines'                                                                as lines,
			                      tags -> 'lit'                                                                  as lit,
			                      tags -> 'maxspeed'                                                             as maxspeed,
			                      tags -> 'crossing:island'                                                      as crossing_island,
			                      tags -> 'traffic_calming'                                                      as traffic_calming,
			                      tags -> 'crossing'                                                             as crossing
			               from hist
			               where (tags ? 'highway'
				               or tags ? 'lines'
				               or tags ? 'lit'
				               or tags ? 'maxspeed'
				               or tags ? 'crossing:island'
				               or tags ? 'traffic_calming'
				               or tags ? 'crossing')
				             and st_intersects(point, (select way from places where admin_level = 2 limit 1))
		               ) T1
	          ) T2
	     group by id, n_name, day, osm_user, mode) T3
group by t3.day, t3.mode;

--------------------------------------
-- 4я страничка
select t.name,
       t3.day,
       T3.mode,
       array_sort_unique(array_agg(T3.osm_user)) as users,
       sum(T3.road_chng)                         as road_chng,
       sum(T3.bus_stop_chng)                     as bus_stop_chng,
       sum(T3.hw_bus_stop_chng)                  as hw_bus_stop_chng,
       sum(T3.lines_chng)                        as lines_chng,
       sum(T3.lit_chng)                          as lit_chng,
       sum(T3.maxspeed_chng)                     as maxspeed_chng,
       sum(T3.crossing_island_chng)              as crossing_island_chng,
       sum(T3.traffic_calming_chng)              as traffic_calming_chng,
       sum(T3.crossing_chng)                     as crossing_chng
from (
	     select id,
	            n_name,
	            day,
	            osm_user,
	            mode,
	            max(version)               as version,
	            array_agg(roads)           as roads,
	            sum(road_chng)             as road_chng,
	            array_agg(bus_stop)        as bus_stop,
	            sum(bus_stop_chng)         as bus_stop_chng,
	            array_agg(hw_bus_stop)     as hw_bus_stop,
	            sum(hw_bus_stop_chng)      as hw_bus_stop_chng,
	            array_agg(lines)           as lines,
	            sum(lines_chng)            as lines_chng,
	            array_agg(lit)             as lit,
	            sum(lit_chng)              as lit_chng,
	            array_agg(maxspeed)        as maxspeed,
	            sum(maxspeed_chng)         as maxspeed_chng,
	            array_agg(crossing_island) as crossing_island,
	            sum(crossing_island_chng)  as crossing_island_chng,
	            array_agg(traffic_calming) as traffic_calming,
	            sum(traffic_calming_chng)  as traffic_calming_chng,
	            array_agg(crossing)        as crossing,
	            sum(crossing_chng)         as crossing_chng,
	            st_union(array_agg(point))    points
	     from (
		          select day,
		                 id,
		                 version,
		                 mode,
		                 n_name,
		                 osm_user,
		                 roads           as roads,
		                 case
			                 when roads is not null and
			                      (roads <> lag(roads) over (partition by id order by version) or lag(roads) over (partition by id order by version) is null)
				                 then 1
			                 else 0 end  as road_chng,
		                 bus_stop        as bus_stop,
		                 case
			                 when bus_stop is not null and
			                      (bus_stop <> lag(bus_stop) over (partition by id order by version) or lag(bus_stop) over (partition by id order by version) is null)
				                 then 1
			                 else 0 end  as bus_stop_chng,
		                 hw_bus_stop     as hw_bus_stop,
		                 case
			                 when hw_bus_stop is not null and
			                      (hw_bus_stop <> lag(hw_bus_stop) over (partition by id order by version) or lag(hw_bus_stop) over (partition by id order by version) is null)
				                 then 1
			                 else 0 end  as hw_bus_stop_chng,
		                 lines           as lines,
		                 case
			                 when lines is not null and
			                      (lines <> lag(lines) over (partition by id order by version) or lag(lines) over (partition by id order by version) is null)
				                 then 1
			                 else 0 end  as lines_chng,
		                 lit             as lit,
		                 case
			                 when lit is not null and
			                      (lit <> lag(lit) over (partition by id order by version) or lag(lit) over (partition by id order by version) is null)
				                 then 1
			                 else 0 end  as lit_chng,
		                 maxspeed        as maxspeed,
		                 case
			                 when maxspeed is not null and
			                      (maxspeed <> lag(maxspeed) over (partition by id order by version) or lag(maxspeed) over (partition by id order by version) is null)
				                 then 1
			                 else 0 end  as maxspeed_chng,
		                 crossing_island as crossing_island,
		                 case
			                 when crossing_island is not null and
			                      (crossing_island <> lag(crossing_island) over (partition by id order by version) or lag(crossing_island) over (partition by id order by version) is null)
				                 then 1
			                 else 0 end  as crossing_island_chng,
		                 traffic_calming as traffic_calming,
		                 case
			                 when traffic_calming is not null and
			                      (traffic_calming <> lag(traffic_calming) over (partition by id order by version) or lag(traffic_calming) over (partition by id order by version) is null)
				                 then 1
			                 else 0 end  as traffic_calming_chng,
		                 crossing        as crossing,
		                 case
			                 when crossing is not null and
			                      (crossing <> lag(crossing) over (partition by id order by version) or lag(crossing) over (partition by id order by version) is null)
				                 then 1
			                 else 0 end  as crossing_chng,
		                 point
		          from (
			               select osm_user,
			                      id,
			                      version,
			                      n_name,
			                      mode,
			                      to_char(timestamp, 'DD.MM')                                                    as day,
			                      case
				                      when tags -> 'highway' = any (ARRAY ['motorway', 'trunk', 'primary', 'secondary', 'tertiary', 'tertiary_link', 'unclassified',
					                      'living_street', 'residential', 'motorway_link', 'trunk_link', 'primary_link', 'secondary_link', 'service']::TEXT[]) then tags -> 'highway'
				                      else null end                                                              as roads,
			                      case when tags -> 'highway' = 'bus_stop' then tags -> 'highway' else null end  as bus_stop,
			                      case when tags -> 'crossing' = 'bus_stop' then tags -> 'highway' else null end as hw_bus_stop,
			                      tags -> 'lines'                                                                as lines,
			                      tags -> 'lit'                                                                  as lit,
			                      tags -> 'maxspeed'                                                             as maxspeed,
			                      tags -> 'crossing:island'                                                      as crossing_island,
			                      tags -> 'traffic_calming'                                                      as traffic_calming,
			                      tags -> 'crossing'                                                             as crossing,
			                      point
			               from hist
			               where (tags ? 'highway'
				               or tags ? 'lines'
				               or tags ? 'lit'
				               or tags ? 'maxspeed'
				               or tags ? 'crossing:island'
				               or tags ? 'traffic_calming'
				               or tags ? 'crossing')
				             and st_intersects(point, (select way from places where admin_level = 2 limit 1))
		               ) T1
	          ) T2
	     group by id, n_name, day, osm_user, mode) T3
	     join towns t
	          on st_intersects(points, t.way) and t.way_area >= 34434850.9309865
group by t.name, t3.day, t3.mode;

----------------------

select T3.osm_user,
       T3.mode,
       sum(T3.road_chng)            as road_chng,
       sum(T3.bus_stop_chng)        as bus_stop_chng,
       sum(T3.hw_bus_stop_chng)     as hw_bus_stop_chng,
       sum(T3.lines_chng)           as lines_chng,
       sum(T3.lit_chng)             as lit_chng,
       sum(T3.maxspeed_chng)        as maxspeed_chng,
       sum(T3.crossing_island_chng) as crossing_island_chng,
       sum(T3.traffic_calming_chng) as traffic_calming_chng,
       sum(T3.crossing_chng)        as crossing_chng
from (
	     -- Получаем суммы изменений в разрезе участник/день по геометриям.
	     select osm_user,
	            mode,
	            sum(road_chng)            as road_chng,
	            sum(bus_stop_chng)        as bus_stop_chng,
	            sum(hw_bus_stop_chng)     as hw_bus_stop_chng,
	            sum(lines_chng)           as lines_chng,
	            sum(lit_chng)             as lit_chng,
	            sum(maxspeed_chng)        as maxspeed_chng,
	            sum(crossing_island_chng) as crossing_island_chng,
	            sum(traffic_calming_chng) as traffic_calming_chng,
	            sum(crossing_chng)        as crossing_chng
	     from (
		          --Из первички выделяем изменения по интересующим нас тегам и высотавляем соответствующий флаг
		          select id,
		                 version,
		                 mode,
		                 osm_user,
		                 case
			                 when roads is not null and
			                      (roads <> lag(roads) over (partition by id order by version) or lag(roads) over (partition by id order by version) is null)
				                 then 1
			                 else 0 end as road_chng,
		                 case
			                 when bus_stop is not null and
			                      (bus_stop <> lag(bus_stop) over (partition by id order by version) or lag(bus_stop) over (partition by id order by version) is null)
				                 then 1
			                 else 0 end as bus_stop_chng,
		                 case
			                 when hw_bus_stop is not null and
			                      (hw_bus_stop <> lag(hw_bus_stop) over (partition by id order by version) or lag(hw_bus_stop) over (partition by id order by version) is null)
				                 then 1
			                 else 0 end as hw_bus_stop_chng,
		                 case
			                 when lines is not null and
			                      (lines <> lag(lines) over (partition by id order by version) or lag(lines) over (partition by id order by version) is null)
				                 then 1
			                 else 0 end as lines_chng,
		                 case
			                 when lit is not null and
			                      (lit <> lag(lit) over (partition by id order by version) or lag(lit) over (partition by id order by version) is null)
				                 then 1
			                 else 0 end as lit_chng,
		                 case
			                 when maxspeed is not null and
			                      (maxspeed <> lag(maxspeed) over (partition by id order by version) or lag(maxspeed) over (partition by id order by version) is null)
				                 then 1
			                 else 0 end as maxspeed_chng,
		                 case
			                 when crossing_island is not null and
			                      (crossing_island <> lag(crossing_island) over (partition by id order by version) or lag(crossing_island) over (partition by id order by version) is null)
				                 then 1
			                 else 0 end as crossing_island_chng,
		                 case
			                 when traffic_calming is not null and
			                      (traffic_calming <> lag(traffic_calming) over (partition by id order by version) or lag(traffic_calming) over (partition by id order by version) is null)
				                 then 1
			                 else 0 end as traffic_calming_chng,
		                 case
			                 when crossing is not null and
			                      (crossing <> lag(crossing) over (partition by id order by version) or lag(crossing) over (partition by id order by version) is null)
				                 then 1
			                 else 0 end as crossing_chng
		          from (
			               -- Первичка. Оббираем только те изменения, которые находятся на территории РФ и только с даты старта конкурса
			               select osm_user,
			                      id,
			                      version,
			                      mode,
			                      case
				                      when tags -> 'highway' = any (ARRAY ['motorway', 'trunk', 'primary', 'secondary', 'tertiary', 'tertiary_link', 'unclassified',
					                      'living_street', 'residential', 'motorway_link', 'trunk_link', 'primary_link', 'secondary_link', 'service']::TEXT[]) then tags -> 'highway'
				                      else null end                                                              as roads,
			                      case when tags -> 'highway' = 'bus_stop' then tags -> 'highway' else null end  as bus_stop,
			                      case when tags -> 'crossing' = 'bus_stop' then tags -> 'highway' else null end as hw_bus_stop,
			                      tags -> 'lines'                                                                as lines,
			                      tags -> 'lit'                                                                  as lit,
			                      tags -> 'maxspeed'                                                             as maxspeed,
			                      tags -> 'crossing:island'                                                      as crossing_island,
			                      tags -> 'traffic_calming'                                                      as traffic_calming,
			                      tags -> 'crossing'                                                             as crossing
			               from hist
			               where (tags ? 'highway'
				               or tags ? 'lines'
				               or tags ? 'lit'
				               or tags ? 'maxspeed'
				               or tags ? 'crossing:island'
				               or tags ? 'traffic_calming'
				               or tags ? 'crossing')
				             and st_within(point, (select way from places where admin_level = 2 limit 1))
				             and timestamp >= '10.09.2021'::TIMESTAMP
		               ) T1
	          ) T2
	     where osm_user = ANY (array [
		     'OSM User','Kachkaev','fndoder','New-Yurok','Timur Gilfanov','evgenykatyshev','ceekay80','Shoorick','Uralla','Viacheslav Gavrilov','Антон007','AlexanderZuevVlg','ks1v',
	         'Alexey Sirotkin','George897','vgrekhov','sikmir','Никитин Николай','AleksandrB007','literan','UrbanGleb','mishaulitskiy','ВаленВ','weary_cynic','lord_Alpaca','pudov_maxim',
	         'n-klyshko','Глеб','borkabritva','AleksandrB007','Васильев Андрей','SoNick_RND','dom159','pfg21','Ignatych','Shortparis','fofanovamv','Shkoda_Ula','AsySmile','Daniil M',
	         'Calmarina ','ZinaidaZ','polkadastr','Vs_80','Dunaich','Corsa5','Заур Гитинов','Айнур Мифтахов','Polina Korolchuk','Екатерина Леттиева','Александр Камашев','nonameforme',
	         'toxat','Nika_no','Evgeniy Starobinets','Allin_kindA','Андрей_Горшков','Фред-Продавец звёзд','4monstrik ','Vs_80','Alex94','flexagoon','Qwertin','annsk29','yorik_iv',
	         'Сержикulsk','Vanchelo','Sanchos159','ЭндрюГарфилд','VRSS','Леонид1997','Olga_E','tuhgus','Som','Aluxantis','mttanya','Anatoliy Prav','messof_anegovich','Sophie_Framboise',
	         'Roma_Boyko','byBAC','ortieom','gulya_akh','polich','classistrip','MadFatVlad','konovalova2207','IliaKrest','TellauR','Luis_Giedroyc','shvedovskiy','ольга сновская','Morkou',
	         'RadmirSad','o4en_moryak','Аханов Дмитрий','Куртуков Константин','dbrwin','Васи Лий','kangu10','qut','sin(x)','pe_skin','panaramix','ra44o','rukus97','adonskoi','Flonger',
	         'GermanLip','ALeXiOZZZ','alexashh','DvinaLand','Валерий Зубанов','0ddo','CheySer','Jane Freud','ArsGaz','Nik Kras','nikitadnlnk','ItzVektor','Sadless74','beer_absorber',
	         'silverst0ne','Rybaso','p_cepheus','sawser','Дядя Ваня Saw','Zacky27','ksvl','Arrrriva','earnestmaps','Василий Александров','Alekzzander','az09','Sandrro','Марья Самородская',
	         '_COOLer_','Rudennn','simsanutiy','PashArt','sqopa','Daniil M','Ольга Коняева','voilashechkin','dmitryborzenkov','rasscrom','AnnMaps2019','alexcheln','Zema34','Augusto Pinochet',
	         'Халида1','Анна Алексеева ','lembit@bitrix24.ru','Ержанчик','VORON_SPb','dergilyova','epidzhx','Laperuza712','maslinych','ekaterina_tei','dezdichado','mira5o','Sipina anastasiya',
	         'нету','11daf','Аня Жаксыбаева','Alex_Boro','filippov70','Ser9ei','Ilya Dontsov','yudin_aa','Const@nt','Lichtenblaster','germangac','Loskir','mitya9697','Z_phyr','Ser9ei',
	         'Woislav','Ershovns','DmitriySaw','vlalexey','Nikolay Podoprigora','Ln13','yaKonovalov','MatveyBub','arina_kamm','Beshanian','pesec','nagor_ant','Artur Petrosian','Sysertchanin',
	         'МаринаZhucheva','j9j','Netdezhda','Георгий412','k7223451','Lazarevsk','rybkolub','Elemzal','petelya','doofyrocks','bravebug','vrodetema','prmkzn','TrickyFoxy','vnogetopor',
	         'Snaark','jmty8','serega1103','cupivan','DeKaN','YatsenkoAnton','Yury Kryvashei','Alexander Rublewski','Srojeco','sph_cow','fendme','Mapkbee','haskini','LeenHis','BCNorwich',
	         'Dzindevis','moonstar-kate','Alexey369i','NetJorika','Роман Платунов','alena_light28','d1sr4n','pacman541','d1sr4n','VlIvYur','Ranas','mrKPbIS','klrnv','Lina_Shpileva_GIS',
	         'dannyloumax','Владимир К','mshilova2003','wosk','Илюха2012','playerr17 ','ArtemVart','Laavang','posts2000','evgenia_osm','subjectnamehere','АлисаЛалиса','Belousov Aleksei',
	         'Kuri Mudro','VdmrO','Kron418','alexeybelyalov','alievahmed','sergsmx','kasimov.an@gmail.com','BusteR2712','MaksN','EkaterinaStepurko','Pashandy','Daria0101','Глеб Серебряный',
	         'Teirudeag','Дарья Брондзя','tanyalotsman','pgisinet','etelinda1986','b00','Alex77','FundaYury','vlllaza','alex_cherrie','2albert','не регистрируется','Полина Игнатенко','Skro11',
	         'Denis_Bakharev_98','alexander_mart','Троллейбус','deidpw','frolove','-off-','ivanbezin','Gorovaja','Arekasu','ValenVolg','полина шурупова','Матвей Гомон','fotohotkovo',
	         'Vasiliy Nazarov','EvgeniyV95','EvgeniyV95','BearDan','polinapeshko','Chingis0811','Kira Utred','Vvo82','an_gorb','nastiaboyne','Анастасия Кононова ','Raul_Tejada','AmritaDhali',
	         'Vlad_Suz','was303'
		     ]::text[])
	     group by id, osm_user, mode) T3
group by t3.osm_user, t3.mode;


-----------

--Из первички выделяем изменения по интересующим нас тегам и выставляем соответствующий флаг
select id,
       version,
       mode,
       osm_user,
       case
	       when roads is not null and
	            (roads <> lag(roads) over (partition by id order by version) or lag(roads) over (partition by id order by version) is null)
		       then 1
	       else 0 end as road_chng,
       case
	       when bus_stop is not null and
	            (bus_stop <> lag(bus_stop) over (partition by id order by version) or lag(bus_stop) over (partition by id order by version) is null)
		       then 1
	       else 0 end as bus_stop_chng,
       case
	       when hw_bus_stop is not null and
	            (hw_bus_stop <> lag(hw_bus_stop) over (partition by id order by version) or lag(hw_bus_stop) over (partition by id order by version) is null)
		       then 1
	       else 0 end as hw_bus_stop_chng,
       case
	       when lines is not null and
	            (lines <> lag(lines) over (partition by id order by version) or lag(lines) over (partition by id order by version) is null)
		       then 1
	       else 0 end as lines_chng,
       case
	       when lit is not null and
	            (lit <> lag(lit) over (partition by id order by version) or lag(lit) over (partition by id order by version) is null)
		       then 1
	       else 0 end as lit_chng,
       case
	       when maxspeed is not null and
	            (maxspeed <> lag(maxspeed) over (partition by id order by version) or lag(maxspeed) over (partition by id order by version) is null)
		       then 1
	       else 0 end as maxspeed_chng,
       case
	       when crossing_island is not null and
	            (crossing_island <> lag(crossing_island) over (partition by id order by version) or lag(crossing_island) over (partition by id order by version) is null)
		       then 1
	       else 0 end as crossing_island_chng,
       case
	       when traffic_calming is not null and
	            (traffic_calming <> lag(traffic_calming) over (partition by id order by version) or lag(traffic_calming) over (partition by id order by version) is null)
		       then 1
	       else 0 end as traffic_calming_chng,
       case
	       when crossing is not null and
	            (crossing <> lag(crossing) over (partition by id order by version) or lag(crossing) over (partition by id order by version) is null)
		       then 1
	       else 0 end as crossing_chng
from (
	     -- Первичка. Оббираем только те изменения, которые находятся на территории РФ и только с даты старта конкурса
	     select osm_user,
	            id,
	            version,
	            mode,
	            case
		            when tags -> 'highway' = any (ARRAY ['motorway', 'trunk', 'primary', 'secondary', 'tertiary', 'tertiary_link', 'unclassified',
			            'living_street', 'residential', 'motorway_link', 'trunk_link', 'primary_link', 'secondary_link', 'service']::TEXT[]) then tags -> 'highway'
		            else null end                                                              as roads,
	            case when tags -> 'highway' = 'bus_stop' then tags -> 'highway' else null end  as bus_stop,
	            case when tags -> 'crossing' = 'bus_stop' then tags -> 'highway' else null end as hw_bus_stop,
	            tags -> 'lines'                                                                as lines,
	            tags -> 'lit'                                                                  as lit,
	            tags -> 'maxspeed'                                                             as maxspeed,
	            tags -> 'crossing:island'                                                      as crossing_island,
	            tags -> 'traffic_calming'                                                      as traffic_calming,
	            tags -> 'crossing'                                                             as crossing
	     from hist
	     where (tags ? 'highway'
		     or tags ? 'lines'
		     or tags ? 'lit'
		     or tags ? 'maxspeed'
		     or tags ? 'crossing:island'
		     or tags ? 'traffic_calming'
		     or tags ? 'crossing')
		   and st_within(point, (select way from places where admin_level = 2 limit 1))
		   and timestamp >= '10.09.2021'::TIMESTAMP
     ) T1

---------------

select id,
       version,
       tags,
       st_geometrytype(point),
       st_transform(point, 4326)                                                  point,
       st_transform(st_makeLine(array(select (st_dumppoints(point)).geom)), 4326) line
from hist
where n_name = 'way'
  and st_geometryType(point) = 'ST_MultiPoint'
  and st_within(point, (select way from places where admin_level = '2'))
order by id, version
limit 100;

select osm_user, /*to_char(timestamp, 'DD.MM') as day,*/ count(*)
from hist
where tags ? 'maxspeed:type'
  and osm_user = ANY (array [
	'Kachkaev','fndoder','New-Yurok','evgenykatyshev','ceekay80','Shoorick','Uralla','Антон007','AlexanderZuevVlg','ks1v','Alexey Sirotkin','vgrekhov','Никитин Николай',
	'AleksandrB007','literan', 'weary_cynic','pudov_maxim','n-klyshko','Глеб','SoNick_RND','pfg21','fofanovamv','Shkoda_Ula','AsySmile','Daniil M','ZinaidaZ','Corsa5',
	'nonameforme','toxat','Alex94','flexagoon','Qwertin', 'VRSS','Som','mttanya','Anatoliy Prav','messof_anegovich','ortieom','gulya_akh','MadFatVlad','TellauR','Luis_Giedroyc',
	'shvedovskiy','Morkou','o4en_moryak','qut','sin(x)','panaramix','GermanLip', 'alexashh','DvinaLand','Валерий Зубанов','0ddo','CheySer','Jane Freud','ArsGaz','Nik Kras',
	'ItzVektor','Sadless74','beer_absorber','sawser','ksvl','Василий Александров','Alekzzander','az09','Марья Самородская','_COOLer_','Rudennn','dmitryborzenkov','AnnMaps2019',
	'Zema34','VORON_SPb','maslinych','Sipina anastasiya','11daf','filippov70','Ser9ei','Ilya Dontsov','yudin_aa','Const@nt','Loskir','Z_phyr','vlalexey','Nikolay Podoprigora',
	'yaKonovalov','arina_kamm','pesec','j9j','Netdezhda','Георгий412','Lazarevsk','doofyrocks','bravebug','prmkzn','TrickyFoxy','vnogetopor','Snaark','jmty8', 'serega1103','DeKaN',
	'YatsenkoAnton','Yury Kryvashei','sph_cow','BCNorwich','Dzindevis','Alexey369i','NetJorika','d1sr4n','pacman541','VlIvYur','Ignatych','Shortparis','IliaKrest','Ln13','LeenHis',
	'mrKPbIS','dannyloumax','Владимир К','mshilova2003','wosk','Илюха2012','playerr17','Laavang','CupIvan','posts2000','alexeybelyalov','alievahmed','sergsmx','BusteR2712','MaksN',
	'EkaterinaStepurko','Daria0101','Timur Gilfanov','Viacheslav Gavrilov','George897','sikmr','UrbanGleb','mishaulitskiy','ВаленВ','lord_Alpaca','borkabritva','Васильев Андрей',
	'dom159','Calmarina','polkadastr','Vs_80','Dunaich','Заур Гитинов','Айнур Мифтахов','Polina Korolchuk','Екатерина Леттиева','Александр Камашев','Nika_no','Evgeniy Starobinets',
	'Allin.kindA','Андрей_Горшков','Фред-Продавец звёзд','4monstrik', 'ann.sk29','yorik_iv','Сержикulsk','Vanchelo','Sanchos159','ЭндрюГарфилд','Леонид1997','Olga_E','tuhgus',
	'Aluxantis','Sophie_Framboise','Roma_Boyko','byBAC','polich','@classistrip','konovalova2207','ольга сновская','RadmirSad','Аханов Дмитрий','Куртуков Константин','dbrwin',
	'Васи Лий','kangu10','pe_skin','ra44o','rukus97','adonskoi','Flonger','ALeXiOZZZ','nikitadnlnk','silverst0ne','Rybaso','p_cepheus','Дядя Ваня Saw','Zacky27','Arrrriva',
	'earnestmaps','Sandrro','simsanutiy','PashArt','sqopa','Ольга Коняева','voilashechkin','rasscrom','alexcheln','Augusto Pinochet','Халида1','Анна Алексеева','lembit@bitrix24.ru',
	'Ержанчик','dergilyova','epidzhx','Laperuza712','ekaterina_tei','dezdichado','mira5o','нету','Аня Жаксыбаева','Alex_Boro','Lichtenblaster','germangac','mitya9697','Woislav',
	'Ershovns','DmitriySaw','MatveyBub','Beshanian','nagor_ant','Artur Petrosian','Sysertchanin','МаринаZhucheva','k7223451','rybkolub','Elemzal','petelya','vrodetema',
	'Alexander Rublewski','Belousov Alexis','fendme','Mapkbee','haskini','moonstar-kate','Роман Платунов','alena_light28','Ranas','klrnv','Lina_Shpileva_GIS','ArtemVart',
	'evgenia_osm','subjectnamehere','АлисаЛалиса','Kuri Mudro','VdmrO','Kron418','kasimov.an@gmail.com','Pashandy','Глеб Серебряный','Teirudeag','Дарья Брондзя','tanyalotsman',
	'pgisinet','etelinda1986','Alex77','FundaYury','alex_cherrie','2albert','не регистрируется','Полина Игнатенко','Skro11','Denis_Bakharev_98','alexander_mart','Троллейбус',
	'deidpw','frolove','-off-','ivanbezin','Gorovaja','Arekasu','ValenVolg','полина шурупова','Матвей Гомон','Vasiliy Nazarov','EvgeniyV95','BearDan','polinapeshko',
	'Chingis0811','Kira Utred','Vvo82','an_gorb'
	]::text[])
and timestamp >= '10.09.2021'::timestamp
group by osm_user/*, to_char(timestamp, 'DD.MM')*/;
