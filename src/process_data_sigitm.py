from pathlib import Path
from platformdirs import user_downloads_dir
import warnings
import pandas as pd
from datetime import datetime
from typing import Optional, Tuple
from dataclasses import dataclass
from .syslog import SystemLogger

@dataclass
class FileProcessingResult:
    success: bool
    message: str
    dataframe: Optional[pd.DataFrame] = None

class ExcelFileHandler:
    """Handler para processamento de arquivos Excel com prefixo específico.
    
    Attributes:
        directory (Path): Diretório para busca dos arquivos
        prefix (str): Prefixo dos arquivos a serem processados
        column_mapping (Dict[str, str]): Mapeamento de colunas para renomeação
        date_columns (Tuple[str, ...]): Colunas que devem ser tratadas como datas
    """

    COLUMN_MAPPING = {
        'Data Criacao': 'data_criacao',
        'VTA PK': 'vta_pk',
        'Raiz': 'raiz',
        'Tíquete Referência': 'tiquete_referencia',
        'Tipo de Bilhete': 'tipo_de_bilhete',
        'Tipo de Alarme': 'tipo_de_alarme',
        'Tipo de Afetação': 'tipo_de_afetacao',
        'Tipo TA': 'tipo_ta',
        'Tipo de Planta': 'tipo_de_planta',
        'Código Localidade': 'codigo_localidade',
        'Sigla Estado': 'sigla_estado',
        'Sigla Município': 'sigla_municipio',
        'Nome Município': 'nome_municipio',
        'Bairro': 'bairro',
        'Código Site': 'codigo_site',
        'Sigla Site V2': 'sigla_site_v2',
        'Empresa Manutenção': 'empresa_manutencao',
        'Grupo Responsavel': 'grupo_responsavel',
        'Status': 'status',
        'Data de Baixa': 'data_de_baixa',
        'Data Encerramento': 'data_encerramento',
        'Observação Histórico': 'observacao_historico' 
    }

    PREFIX = "CONSULTA_TLP_PCP_CS"

    DATE_COLUMNS = ("data_criacao", "data_de_baixa", "data_encerramento")
    DATETIME_FORMAT = "%d%m%y_%H%M"
    DATETIME_FORMAT_ISO = '%Y-%m-%d %H:%M:%S'
    DISPLAY_DATETIME_FORMAT = "%Y-%m-%d %H:%M"
    DISPLAY_DATE_FORMAT = "%Y-%m-%d"

    def __init__(self, directory: Optional[Path] = None, prefix: str = PREFIX):
        """Inicializa o handler com diretório e prefixo.
        
        Args:
            directory: Diretório para busca. Padrão: diretório de downloads do usuário
            prefix: Prefixo dos arquivos a serem processados
        """

        self.directory = Path(directory) if directory else Path(user_downloads_dir())
        self.prefix = prefix
        self.logger = SystemLogger.configure_logger("ExcelFileHandler")

        warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)

        if not self.directory.exists():
            self.logger.warning(f"Diretório não encontrado: {self.directory}")
            self.directory.mkdir(parents=True, exist_ok=True)
            self.logger.info(f"Diretório criado: {self.directory}")
    
    def _find_most_recent_file(self) -> Optional[Path]:
        """Encontra o arquivo mais recente com o prefixo configurado.
        
        Returns:
            Path do arquivo mais recente ou None se não encontrado
            
        Raises:
            FileNotFoundError: Se nenhum arquivo for encontrado
        """

        search_pattern = f"{self.prefix}*"
        files = list(self.directory.glob(search_pattern))

        if not files:
            self.logger.error(f"Nenhum arquivo encontrado com o prefixo: {self.prefix}")
            raise FileNotFoundError(f"Nenhum arquivo com prefixo {self.prefix} encontrado em {self.directory}")
        
        return max(files, key=lambda f: f.stat().st_mtime)
    
    def _extract_datetime_from_filename(self, file_path: Path) -> Tuple[str, str]:
        """Extrai data e hora do nome do arquivo.
        
        Args:
            file_path: Path do arquivo
            
        Returns:
            Tupla com (data_formatada, datetime_formatado)
            
        Raises:
            ValueError: Se o formato da data no nome do arquivo for inválido
        """
        import re

        try:
            file_name = file_path.stem

            # Busca o padrão de 6 dígitos, underscore, 4 dígitos (ex: 040126_1212)
            match = re.search(r'(\d{6}_\d{4})', file_name)

            if not match:
                raise ValueError(f"Padrão de data não encontrado em: {file_name}")
            
            datetime_str = match.group(1)
            dt_obj = datetime.strptime(datetime_str, self.DATETIME_FORMAT)

            return(dt_obj.strftime(self.DISPLAY_DATE_FORMAT), dt_obj.strftime(self.DISPLAY_DATETIME_FORMAT))

        except Exception as e:
            self.logger.error(f"Erro na extração: {e}")
            raise   
    
    def _process_dataframe(self, df: pd.DataFrame, file_path: Path) -> pd.DataFrame:
        """Processa o dataframe com transformações necessárias.
        
        Args:
            df: DataFrame original
            file_path: Path do arquivo para extrair metadados
            
        Returns:
            DataFrame processado
        """
        # Renomeia colunas
        df = df.rename(columns=self.COLUMN_MAPPING)

        # Adiciona colunas de carga
        dt_carga, dthr_carga = self._extract_datetime_from_filename(file_path)
        df.insert(0, "dt_carga", dt_carga)
        df.insert(1, "dthr_carga", dthr_carga)

        # Processa colunas de data
        for col in self.DATE_COLUMNS:
            if col in df.columns:
                try:
                    with warnings.catch_warnings():
                        warnings.simplefilter('ignore', UserWarning)
                        # Converte a string original para objeto datetime do Pandas
                        df[col] = pd.to_datetime(df[col], errors='coerce')
                    
                    # Converte o objeto datetime para string no formato ISO (yyyy-mm-dd hh:mm:ss)
                    df[col] = df[col].dt.strftime(self.DATETIME_FORMAT_ISO).where(df[col].notnull(), None)
                
                except Exception as e:
                    self.logger.warning(f"⚠️ Erro no processamento da coluna {col}: {e}")

        # Tratamento de IDs e tipagem Segura        
        id_cols = ["vta_pk", "raiz", "codigo_localidade"]

        for col in id_cols:
            if col in df.columns:
                df[col] = df[col].fillna(0).astype('Int64').astype(str).replace('0', None)
        
        # Limpeza Final (Substitui 'NaT' gerado pelo strftime por None)
        df = df.replace({pd.NA: None, "nan": None, "None": None, "": None, "NaT": None})
        
        # Normalização de colunas de texto
        text_cols = df.select_dtypes(include=['object']).columns
        df[text_cols] = df[text_cols].astype(str).replace("None", None)

        return df
    
    def _load_to_dataframe(self, file_path: Path) -> FileProcessingResult:
        """Carrega arquivos Excel """
        try:

            df = pd.read_excel(file_path, engine='calamine')

            processed_df = self._process_dataframe(df, file_path)

            self.logger.info("Arquivo Excel processado com sucesso.")
            
            return FileProcessingResult(success=True, message="Arquivo processado com sucesso", dataframe=processed_df)
        
        except Exception as e:
            return FileProcessingResult(success=False, message=f"Erro ao processar arquivo: {str(e)}") 

    def process_most_recent_file(self, file_path) -> FileProcessingResult:
        """Processa o arquivo mais recente encontrado.
        
        Returns:
            FileProcessingResult com status e dados
        """

        try:
            # Se o orquestrador já possui o caminho, usa ele. Do contrário, busca no disco (fallback).
            target_path = file_path if file_path else self._find_most_recent_file()

            self.logger.info(f"🎯 Alvo de processamento: {target_path.name}")
            return self. _load_to_dataframe(target_path)

        except Exception as e:
            self.logger.error(f"Erro ao processar arquivo mais recente: {e}")
            return FileProcessingResult(success=False, message=f"Erro ao processar arquivo mais recente: {str(e)}")

    def delete_most_recent_file(self, file_path) -> bool:
        """Remove o arquivo mais recente encontrado.
        
        Returns:
            bool: True se removido com sucesso, False caso contrário
        """
        try:
            # Se o orquestrador já possui o caminho, usa ele. Do contrário, busca no disco (fallback).
            target_path = file_path if file_path else self._find_most_recent_file()
            target_path.unlink()
            self.logger.info(f"Arquivo removido com sucesso: {file_path}")
            return True

        except Exception as e:
            self.logger.error(f"Erro ao remover arquivo: {e}")
            return False                                    