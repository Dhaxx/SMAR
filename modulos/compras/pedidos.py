from conexao import *
from ..tools import *
from tqdm import tqdm

PRODUTOS = produtos()
LICITACAO = licitacoes()
CENTROCUSTO = depara_ccusto()

def cabecalho():
    cria_campo('ALTER TABLE cadped ADD af_ant varchar(10)')
    cria_campo('ALTER TABLE cadped ADD nafano_ant varchar(10)')
    cria_campo('ALTER TABLE cadped ADD codgrupo_ant varchar(10)')
    cria_campo('ALTER TABLE cadped ADD anogrupo_ant varchar(10)')
    cria_campo('ALTER TABLE cadped ADD numint_ant varchar(10)')

    cur_fdb.execute('delete from icadped')
    cur_fdb.execute('delete from cadped')

    insert = cur_fdb.prep("""insert into cadped (numped, num, ano, datped, codif, total, entrou, codccusto, id_cadped, 
                                                 empresa, numlic, af_ant, nafano_ant, codgrupo_ant, anogrupo_ant, numint_ant)
                         values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""")
    
    consulta = fetchallmap(f"""select distinct
                                    RIGHT('00000' + cast(b.numint AS varchar),
                                    5)+ '/' + SUBSTRING(b.anoint, 3, 2) numped,
                                    RIGHT('00000' + cast(b.numint as varchar),
                                    5) num,
                                    b.anoint ano,
                                    a.nafdta datped,
                                    a.codfor,
                                    'N' entrou,
                                    c.UnidOrc,
                                    a.id,
                                    cast(a.sigla as varchar)+'-'+cast(a.convit as varchar)+'/'+cast(a.anoc as varchar),
                                    a.sigla,
                                    a.convit,
                                    a.anoc,
                                    a.af af_ant,
                                    a.nafano nafano_ant,
                                    a.codgrupo codgrupo_ant,
                                    a.anogrupo anogrupo_ant,
                                    a.numint numint_ant 
                                from
                                    mat.MCT67000 a
                                join mat.MCT66800 b on
                                    a.numint = b.numint
                                    and a.anoint = b.anoint
                                left join mat.UnidOrcamentariaW c on
                                    a.idNivel5 = c.idNivel5
                                where
                                    a.anoc = {ANO}""")
    
    for row in tqdm(consulta, desc='Pedidos - Cabecalho'):
        numped = row['numped']
        num = row['num']
        ano = row['ano']
        datped = row['datped']
        codif = row['codfor']
        total = '0'
        entrou = row['entrou']
        codccusto = CENTROCUSTO[row['UnidOrc']]
        id_cadped = row['id']
        empresa = EMPRESA
        af_ant = row['af_ant']
        nafano_ant = row['nafano_ant']
        codgrupo_ant = row['codgrupo_ant']
        anogrupo_ant = row['anogrupo_ant']
        numlic = LICITACAO[(row['convit'],row['sigla'],row['anoc'])]
        numint_ant = row['numint_ant']
        cur_fdb.execute(insert,(numped, num, ano, datped, codif, total, entrou, codccusto, id_cadped, 
                                empresa, numlic, af_ant, nafano_ant, codgrupo_ant, anogrupo_ant, numint_ant))
    commit()

def itens():
    cur_fdb.execute('delete from icadped')

    insert = cur_fdb.prep('insert into icadped (numped, item, cadpro, qtd, prcunt, prctot, codccusto, id_cadped) values (?,?,?,?,?,?,?,?)')

    consulta = fetchallmap(f"""select
                                    RIGHT('00000' + cast(b.numint AS varchar),
                                    5)+ '/' + SUBSTRING(b.anoint, 3, 2) numped,
                                    isnull(b.nuitem,
                                    ROW_NUMBER() over(partition by b.numint,
                                    b.anoint
                                order by
                                    b.numint)) nuitem,
                                    b.estrut + '.' + b.grupo + '.' + b.subgrp + '.' + b.itemat + '-' + b.digmat cadpro,
                                    b.qtde,
                                    b.preco,
                                    b.total,
                                    c.UnidOrc,
                                    a.id
                                from
                                    mat.MCT67000 a
                                join mat.MCT66800 b on
                                    a.numint = b.numint
                                    and a.anoint = b.anoint
                                left join mat.UnidOrcamentariaW c on
                                    a.idNivel5 = c.idNivel5
                                where
                                    a.anoc = {ANO}""")
    
    for row in tqdm(consulta, desc='Pedidos - Cadastrando Itens'):
        numped = row['numped']
        item = row['nuitem']
        cadpro = PRODUTOS[row['cadpro']]
        qtd = row['qtde']
        prcunt = row['preco']
        prctot = row['total']
        codccusto = CENTROCUSTO[row['UnidOrc']]
        id_cadped = row['id']
        cur_fdb.execute(insert,(numped, item, cadpro, qtd, prcunt, prctot, codccusto, id_cadped))
    commit()