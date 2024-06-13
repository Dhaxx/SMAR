from modulos import tools, patrimonio, compras, frotas

def main():
##### COMPRAS #####
    # compras.base.cadunimedida()
    # compras.base.grupo_e_subgrupo()
    # compras.base.cadest()
    # compras.base.almoxarifado()
    # compras.base.centro_custo()

    # compras.solicitacoes.cadastro()
    # compras.cotacoes.cadastro()
    # compras.cotacoes.fornecedores()
    # compras.cotacoes.valores()

    # compras.cadlic()
    # compras.cadprolic()
    # tools.fornecedores_gerais()
    # compras.prolic_prolics()
    # compras.cadpro_proposta()
    # compras.cadpro_lance() 
    # compras.cadpro_final() 
    # compras.cadpro_status() 
    # compras.cadpro() 
    # compras.regpreco() 
    # compras.aditamento()
    # compras.cadpro_saldo_ant()
    # compras.fase_iv()
    # compras.vinculacao_contratos()

    # compras.pedidos.cabecalho()
    # compras.pedidos.itens()
    # compras.estoque.subpedidos()

##### ALMOXARIFADO #####   
    # compras.estoque.almoxarif_para_ccusto()
    # compras.estoque.requi_saldo_ant()
    # compras.estoque.requi()

# ##### FROTAS #####
    # conexao.cur_sql.execute('USE SMARfrotas') # Altera o banco de dados para SMARfrotas
    # frotas.motorista.cadastro()
    # frotas.veiculos.modelo()
    # frotas.veiculos.marca()
    # frotas.veiculos.cadastro()   
    # conexao.cur_sql.execute('USE smar_compras') # Altera o banco de dados para smar_compras

##### PATRIMÔNIO #####
    # patrimonio.base.tipos_mov()
    # patrimonio.base.tipos_ajuste()
    # patrimonio.base.tipos_baixa()
    # patrimonio.base.tipos_bens()
    # patrimonio.base.tipos_situacao()
    # patrimonio.base.grupos()
    # patrimonio.base.unidade_subunidade()
    
    # patrimonio.cadastro.bens()
    # patrimonio.movimentacoes.aquisicao()
    # patrimonio.movimentacoes.baixas()
    # patrimonio.movimentacoes.depreciacoes()
    patrimonio.movimentacoes.transferencias()
    
##### AJUSTES #####
    # tools.aditivos_contratos()
    # tools.insere_cadpro_cadped()
    # tools.insere_crc()
    # tools.insere_socios_administradores()
    # tools.datas_cadlic()
    # tools.ajusta_descritivo_cotacoes()
    # tools.vincula_socio_fornecedor()
    # tools.imoveis_info()
    # tools.setores_faltantes()

if __name__ == '__main__':
    main()