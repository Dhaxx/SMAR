from modulos.patrimonio import *

def bens():
    cur_fdb.execute('delete from pt_cadpat')

    consulta = fetchallmap('''SELECT 
                                codigo_pat     = p650.idPatmob,
                                codigo_gru_pat = case when p050.ictipcadastro = 'M' then 1 else 2 end,
                                chapa_pat         = p650.pchapa,
                                codigo_set_pat_pkant   = a.UnidOrc, 
                                nota_pat         = p650.pdocto,
                                orig_pat_pkant   = p650.idformaaquisicao,
                                codigo_for_pat   = rtrim(c.dcto01),
                                codigo_tip_pat   = p650.idclspatrimonial,           		  
                                codigo_sit_pat   = p810.conceito,          
                                discr_pat        = CASE WHEN EXISTS(SELECT 1 FROM mat.MXT70800 WHERE cgccli = '27142058000126') THEN p650.despro ELSE RTRIM(LTRIM(ISNULL(p650.despro,'')))+' '+RTRIM(LTRIM(ISNULL(p650.descr,''))) END,        
                                datae_pat    = p650.pdtqui,        
                                dt_contabil = p650.pdtlib,        
                                dtpag_pat        = p650.datbai,        
                                valaqu_pat       = p650.pvalaq,        
                                valatu_pat       = p650.pvlmes,        
                                valres_pat       = p650.VlrResidual,        
                                tipatr           = p050.ictipcadastro,        
                                TpVidaUtil       = CASE p650.tpvidautil WHEN 'D' THEN 'Definido' ELSE 'Indefinido' END,        
                                percenqtd_pat = p650.qtmesvidamovel,
                                case when p650.tpvidautil = 'D' then 'V' else NULL end DAE_PAT,
                                Situacao         = CASE p650.icsitmovel         
                                                    WHEN 'B' THEN 'Baixado'        
                                                    WHEN 'I' THEN 'Inservível'        
                                                    WHEN 'N' THEN 'Normal'        
                                                    WHEN 'T' THEN 'Transferido'        
                                                    WHEN 'E' THEN 'Estornado'        
                                                END        
                            FROM mat.MPT65000 p650        
                            -- Join com a Classe Patrimonial para identificar se é Móvel ou Veículo        
                            INNER JOIN mat.MPT05000 p050 ON p050.idclspatrimonial      = p650.idclspatrimonial        
                            LEFT JOIN mat.MPT81000 p810 ON p810.idpontfatorinfluencia = p650.idestadocsv   
                            JOIN mat.MXT03700 t037 ON t037.idformaaquisicao = p650.idformaaquisicao
                            join mat.UnidOrcamentariaW a ON a.idNivel5 = p650.idNivel5
                            join mat.MXT60100 b on b.codfor = p650.codfor
                            join mat.mxt61400 c on c.codnom = b.codnom
                            UNION
                            SELECT 
                                codigo_pat        = p657.idPatImob,        
                                codigo_gru_pat = 3,
                                chapa_pat         = p657.pinscr,   
                                codigo_set_pat_pkant   = a.UnidOrc,
                                nota_pat         = rtrim(p657.docto),
                                orig_pat_pkant   = p657.idformaaquisicao,
                                codigo_for_pat   = rtrim(c.dcto01),
                                codigo_tip_pat   = p657.idclspatrimonial,        
                                codigo_sit_pat   = p810.conceito,        
                                discr_pat        = RTRIM(LTRIM(p657.pideno)),        
                                DataAquisicao    = p657.pidta1,        
                                DataIncorporacao = p657.pdtlib,        
                                DataBaixa        = p657.datbai,        
                                valaqu_pat       = p657.pivalt,        
                                valatu_pat       = p657.pvlmes,        
                                valres_pat    = p657.VlrResidual,                     
                                tipatr           = 'I',        
                                TpVidaUtil       = CASE p657.tpvidautil WHEN 'D' THEN 'Definido' ELSE 'Indefinido' END,        
                                percenqtd_pat = p657.qtmesvidaImovel,
                                case when p657.tpvidautil = 'D' then 'V' else NULL end DAE_PAT,      
                                Situacao         = CASE p657.icsitimovel        
                                                    WHEN 'B' THEN 'Baixado'        
                                                    WHEN 'I' THEN 'Inservível'        
                                                    WHEN 'N' THEN 'Normal'        
                                                    WHEN 'T' THEN 'Transferido'        
                                                    WHEN 'E' THEN 'Estornado'        
                                                END   
                            FROM mat.MPT65700 p657          
                            LEFT JOIN mat.MPT81000 p810 ON p810.idpontfatorinfluencia = p657.idestadocsv        
                            left JOIN mat.MPT79900 p799  
                            ON p799.dossie = p657.pinscr  
                            JOIN mat.MXT03700 t037 ON t037.idformaaquisicao = p657.idformaaquisicao
                            join mat.UnidOrcamentariaW a ON a.idNivel5 = p657.idNivel5
                            join mat.MXT60100 b on b.codfor = p657.propriet_ant
                            join mat.mxt61400 c on c.codnom = b.codnom
                            UNION    
                            SELECT 
                                codigo_pat     = p801.idAcervo,
                                codigo_gru_pat = 4 ,
                                chapa_pat         = p801.cod_livro,   
                                codigo_set_pat_pkant   = a.UnidOrc,
                                nota_pat         = rtrim(p801.iditemnf),
                                orig_pat_pkant   = p801.idformaaquisicao,
                                codigo_for_pat   = rtrim(c.dcto01),
                                codigo_tip_pat   = p801.idclspatrimonial,        
                                codigo_sit_pat   = p810.conceito, 
                                discr_pat        = RTRIM(LTRIM(p801.despro)),        
                                DataAquisicao    = p801.data_inc,        
                                DataIncorporacao = p801.data_incorporacao,        
                                DataBaixa        = p801.data_baixa,        
                                valaqu_pat       = p801.valor,        
                                valatu_pat       = p801.vlbem,        
                                valres_pat    = p801.VlrResidual,        
                                tipatr           = 'A',  
                                TpVidaUtil       = CASE p801.tpvidautil WHEN 'D' THEN 'Definido' ELSE 'Indefinido' END,               
                                percenqtd_pat = p801.qtmesvidaacervo,              
                                DAE_PAT       = case when p801.tpvidautil = 'D' then 'V' else NULL end,
                                Situacao         = CASE p801.icsitacervo        
                                                    WHEN 'B' THEN 'Baixado'        
                                    WHEN 'I' THEN 'Inservível'        
                                                    WHEN 'N' THEN 'Normal'        
                                                    WHEN 'T' THEN 'Transferido'        
                                                    WHEN 'E' THEN 'Estornado'        
                                                END		  
                            FROM mat.MPT80100 p801        
                            LEFT JOIN mat.MPT81000 p810 ON p810.idpontfatorinfluencia = p801.idestadocsv   
                            JOIN mat.MXT03700 t037 ON t037.idformaaquisicao = p801.idformaaquisicao
                            join mat.UnidOrcamentariaW a ON a.idNivel5 = p801.idNivel5
                            join mat.MXT60100 b on b.codfor = p801.codfor
                            join mat.mxt61400 c on c.codnom = b.codnom
                            ''')
    
    insert = cur_fdb.prep('''
                          insert
                                into
                                pt_cadpat (codigo_pat,     
                                            codigo_gru_pat, 
                                            chapa_pat,         
                                            codigo_set_pat_pkant,   
                                            nota_pat,         
                                            orig_pat_pkant,   
                                            codigo_for_pat,   
                                            codigo_tip_pat,   
                                            codigo_sit_pat,   
                                            discr_pat,       
                                            DataAquisicao,    
                                            DataIncorporacao,
                                            DataBaixa,        
                                            valaqu_pat,       
                                            valatu_pat,       
                                            valres_pat,    
                                            tipatr,           
                                            TpVidaUtil,       
                                            percenqtd_pat, 
                                            dae_pat,       
                                            situacao)
                            values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
                          ''')
    
    for row in tqdm(consulta, desc='PATRIMONIO - Cadastro de Bens'):
        codigo_pat = row['codigo_pat']
        empresa_pat = EMPRESA
        codigo_gru_pat = row['codigo_gru_pat']
        chapa_pat = row['chapa_pat']
        codigo_cpl_pat = row['codigo_cpl_pat']
        codigo_set_pat = SUBUNIDADES.get(row['codigo_set_pat_pkant'], 0)
        codigo_set_atu_pat = SUBUNIDADES.get(row['codigo_set_pat_pkant'], 0)
        orig_pat = row['orig_pat']
        codigo_tip_pat = row['codigo_tip_pat']
        codigo_sit_pat = row['codigo_sit_pat']
        discr_pat = row['discr_pat']
        obs_pat = row['obsgeral']
        datae_pat = row['datae_pat']
        dtlan_pat = row['dtlan_pat']
        valaqu_pat = row['valaqu_pat']
        valatu_pat = row['valatu_pat']
        codigo_for_pat = row['codigo_for_pat']
        percenqtd_pat = row['percenqtd_pat']
        dae_pat = row['dae_pat']
        valres_pat = row['valres_pat']
        percentemp_pat = row['percentemp_pat']
        codigo_bai_pat = row['codigo_bai_pat']
        dtpag_pat = row['dtpag_pat']
        nota_pat = row['nota_pat']
        cur_fdb.execute(insert,(codigo_pat, empresa_pat, chapa_pat, discr_pat, obs_pat, codigo_gru_pat, orig_pat, 
                                codigo_tip_pat, codigo_sit_pat, codigo_cpl_pat, codigo_for_pat, codigo_set_pat, 
                                codigo_set_atu_pat, valaqu_pat, valatu_pat, percenqtd_pat, dae_pat, valres_pat, 
                                percentemp_pat, datae_pat, dtlan_pat, codigo_bai_pat, dtpag_pat, nota_pat))
    cur_fdb.execute('UPDATE pt_cadpat a SET a.CODIGO_CPL_PAT = (SELECT b.codigo_tce_tip FROM pt_cadtip b WHERE b.codigo_tip = a.CODIGO_TIP_PAT AND b.codigo_tce_tip IS NOT null)')
    commit()