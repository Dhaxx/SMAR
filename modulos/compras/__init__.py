from conexao import *
from tqdm import tqdm
from collections import namedtuple

from modulos.tools import *
from modulos.compras import base
PRODUTOS = produtos()

from modulos.compras import solicitacoes
CENTROCUSTOS = depara_ccusto()

from modulos.compras import cotacoes
COTACAO = lista_cotacoes()

from modulos.compras.licitacao import cadlic
LICITACAO = licitacoes()

from modulos.compras.licitacao import cadprolic
NOME_FORNECEDOR, INSMF_FORNECEDOR = fornecedores()

from modulos.compras.licitacao import prolic_prolics
from modulos.compras.licitacao import cadpro_status
from modulos.compras.licitacao import cadpro_proposta
from modulos.compras.licitacao import cadpro_lance
from modulos.compras.licitacao import cadpro_final
from modulos.compras.licitacao import cadpro
from modulos.compras.licitacao import regpreco
from modulos.compras.licitacao import aditamento
ITEM_PROPOSTA = item_da_proposta()
from modulos.compras.licitacao import cadpro_saldo_ant
from modulos.compras.licitacao import fase_iv
from modulos.compras.licitacao import vinculacao_contratos

from modulos.compras import pedidos
from modulos.compras import estoque
from modulos.compras import frotas