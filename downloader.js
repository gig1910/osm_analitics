import fetch from 'node-fetch';
import zlib  from 'zlib';
import xpath from 'xpath';
import dom   from 'xmldom';
import PG    from 'pg';

const client = new PG.Client({
								 user:     'osm',
								 host:     '127.0.0.1',
								 database: 'gis',
								 password: 'Qwerty21',
								 port:     5432
							 });

function streamToString(stream){
	const chunks = [];
	return new Promise((resolve, reject) => {
		stream.on('data', (chunk) => chunks.push(Buffer.from(chunk)));
		stream.on('error', (err) => reject(err));
		stream.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
	});
}

let _parts = parseInt(process.argv[2], 10) || 1;
let _part = parseInt(process.argv[3], 10) || 0;

(async() => {

	let options = {
		// These properties are part of the Fetch Standard
		method:   'GET',
		headers:  {},            	// Request headers. format is the identical to that accepted by the Headers constructor (see below)
		body:     null,             // Request body. can be null, a string, a Buffer, a Blob, or a Node.js Readable stream
		redirect: 'follow',     	// Set to `manual` to extract redirect headers, `error` to reject redirect
		signal:   null,           	// Pass an instance of AbortSignal to optionally abort requests

		// The following properties are node-fetch extensions
		follow:             20,     // maximum redirect count. 0 to not follow redirect
		compress:           true,   // support gzip/deflate content encoding. false to disable
		size:               0,      // maximum response body size in bytes. 0 to disable
		agent:              null,   // http(s).Agent instance or function that returns an instance (see below)
		highWaterMark:      16384,  // the maximum number of bytes to store in the internal buffer before ceasing to read from the underlying resource.
		insecureHTTPParser: false	// Use an insecure HTTP parser that accepts invalid HTTP headers when `true`.
	};
	//https://planet.maps.mail.ru/replication/minute/004/708/000.osc.gz

	await client.connect();
	try{
		let res = await client.query('select n1, n2 from diff where not state and ((n1*1000 + n2) % $1) = $2 order by 1, 2', [_parts, _part]);
		for(let r = 0; r < res.rows.length; r++){

			let _n1 = ('000' + res.rows[r].n1.toString()).substr(-3);
			let _n2 = ('000' + res.rows[r].n2.toString()).substr(-3);

			let _url = `https://planet.maps.mail.ru/replication/minute/004/${_n1}/${_n2}.osc.gz`;
			console.log('%s %s	Скачиваем: %s', _n1, _n2, _url);

			let _tSt = (new Date()).getTime();
			let response;
			try{
				//Загрузка файла диффа
				response = await fetch(_url, options);
			}catch(err){
				console.error(err);
				process.exit(-300);
			}

			if(!response.ok){
				console.log(`unexpected response ${response.status} - ${response.statusText}`);
				if(response.status >= 400 && response.status < 500){
					console.log('Возможно конец файлов диффа. Выходим');
					process.exit(0);
				}
				if(response.status >= 500 && response.status < 600){
					console.log('Ошибка запроса файла на сервере. Выходим');
					process.exit(-200);
				}
			}

			//Распаковываем архив
			let gunzip = zlib.createGunzip();
			let xml    = await streamToString(response.body.pipe(gunzip));
			xml        = xml.replace(/>\n\s*</igm, '><');

			//Парсим полученный XML
			let _tPr = (new Date()).getTime();
			console.log('%s %s (%ss)\t\tПарсим XML...', _n1, _n2, (_tPr - _tSt) / 1000);

			let XML = (new dom.DOMParser()).parseFromString(xml);

			let _tUp = (new Date()).getTime();
			console.log('%s %s (%ss) (%ss)\t\tЗаливаем в БД...', _n1, _n2, (_tUp - _tSt) / 1000, (_tPr - _tSt) / 1000);

			let els1 = xpath.select('/*/*', XML);
			for(let i = 0; i < els1.length; i++){
				let mode = els1[i].nodeName;
				let els2 = xpath.select('*', els1[i]);
				for(let j = 0; j < els2.length; j++){
					let el     = els2[j];
					let n_name = el.nodeName;
					let id, version, timestamp, uid, user, changeset, lat, lon, tags, nodes, members;

					id        = parseInt(xpath.select('@id', el)[0].value);
					version   = parseInt(xpath.select('@version', el)[0].value);
					timestamp = xpath.select('@timestamp', el)[0].value;
					uid       = parseInt(xpath.select('@uid', el)[0].value);
					user      = xpath.select('@user', el)[0].value;
					changeset = parseInt(xpath.select('@changeset', el)[0].value);

					tags = [];
					xpath.select('tag', el).forEach(el => {
						tags.push(`"${xpath.select('@k', el)[0].value}"=>"${xpath.select('@v', el)[0].value
																									 .replace(/"/igm, '&QUOT;')
																									 .replace(/'/igm, '&APOS;')
																									 .replace(/\\/igm, '\\\\')
						}"`);
					});
					tags = tags.join(',');

					switch(n_name){
						case 'node':
							lat = parseFloat(xpath.select('@lat', el)[0].value);
							lon = parseFloat(xpath.select('@lon', el)[0].value);
							break;

						case 'way':
							nodes = [];
							xpath.select('nd/@ref', el).forEach((el, ind) => nodes.push(parseInt(el.value)));
							nodes = '{' + nodes.join(',') + '}';
							break;

						case 'relation':
							members = [];
							xpath.select('member', el).forEach((el, ind) => members.push({
																							 type: xpath.select('@type', el)[0].value,
																							 ref:  parseInt(xpath.select('@ref', el)[0].value),
																							 role: xpath.select('@role', el)[0].value
																						 }));
							members = JSON.stringify(members, '', '');
							break;
					}

					try{
						await client.query('insert into history (mode, n_name, id, version, timestamp, uid, osm_user, changeset, lat, lon, tags, nodes, members)\n' +
											   'values ($1::TEXT, $2::TEXT, $3::BIGINT, $4::INT, $5::TIMESTAMP, $6::BIGINT, $7::TEXT, $8::INT, $9::NUMERIC, $10::NUMERIC, $11::HSTORE, $12::bigint[], $13::JSONB)\n' +
											   'on conflict do nothing;',
										   [mode, n_name, id, version, timestamp, uid, user, changeset, lat, lon, tags, nodes, members]);
					}catch(err){
						console.error(err);
						console.log(tags);
						console.log(nodes);
						console.log(members);
						process.exit(-100);
					}
				}
			}

			let _tEnd = (new Date()).getTime();
			console.log('%s %s (%ss) (%ss)	готово...', _n1, _n2, (_tEnd - _tSt) / 1000, (_tEnd - _tUp) / 1000);

			await client.query('update diff set state = true, t = now(), work=$3 where n1=$1 and n2 = $2', [res.rows[r].n1, res.rows[r].n2, (_tEnd - _tUp / 1000)]);
		}

	}finally{
		await client.end();
	}
})
();
