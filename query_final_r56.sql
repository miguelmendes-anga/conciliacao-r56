with estoque as(
	WITH max_ref AS (
		SELECT max(dt_referencia)
		FROM db_anga.tbdwr_estoque_administrador
		WHERE fundo = 'fidc-multi-consignados-i'
	)
	SELECT t1.*
	FROM db_anga.tbdwr_estoque_administrador t1
	WHERE fundo = 'fidc-multi-consignados-i'
		AND dt_referencia = (select * from max_ref)
		and nome_cedente like '%UY3%'
)
, protocolos as (
    select numero_documento, max(chave_reserva_cef) chave_reserva_cef, max(numero_protocolo_cef) numero_protocolo_cef from db_anga.tbdwr_protocolos_caixa where numero_documento in (select distinct numero_documento from estoque) group by numero_documento
)
, estoque_com_protocolo as (
    select estoque.*, protocolos.chave_reserva_cef, protocolos.numero_protocolo_cef, concat(protocolos.chave_reserva_cef, '_', date_format(estoque.dt_vencimento, '%Y%m'), '01') chave_r56 from estoque left join protocolos on estoque.numero_documento = protocolos.numero_documento
)
, r56 as (
    SELECT *,
           CONCAT(identificador_solicitacao, '_', DATE_FORMAT(dt_prevista_repasse, '%Y%m%d')) AS chave_r56,
           ROW_NUMBER() OVER (
               PARTITION BY CONCAT(identificador_solicitacao, '_', DATE_FORMAT(dt_prevista_repasse, '%Y%m%d')) 
               ORDER BY status_periodo DESC
           ) AS row_num
    FROM db_anga.tbdwr_arquivo_r56
    WHERE CONCAT(identificador_solicitacao, '_', DATE_FORMAT(dt_prevista_repasse, '%Y%m%d')) IN (
        SELECT DISTINCT chave_r56 
        FROM estoque_com_protocolo
    )
)
, r56_filtrado as(
    select * from r56 where row_num = 1
)
select * from estoque_com_protocolo left join r56_filtrado on estoque_com_protocolo.chave_r56 = r56_filtrado.chave_r56