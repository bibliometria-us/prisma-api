#!/usr/bin/env python3
"""
Monitor de Rate Limit para Scopus API
Verifica el estado de cuotas, peticiones restantes y códigos de error
"""

import requests
import json
from datetime import datetime
from integration.apis.elsevier import config


class ScopusRateLimitMonitor:
    """
    Monitor completo del rate limit de Scopus API
    """
    
    def __init__(self, api_key: str = None):
        self.api_key = api_key or config.api_key
        self.inst_token = config.inst_token_key
        self.base_url = "https://api.elsevier.com"
        self.headers = {
            'X-ELS-APIKey': self.api_key,
            'X-ELS-Insttoken': self.inst_token,
            'Accept': 'application/json'
        }
        
        self.last_response = None
        self.rate_limit_info = {}
        self.error_info = {}
        
    def check_rate_limit_status(self):
        """
        Realiza una petición simple para obtener información de rate limit
        """
        print("🔍 MONITOR DE RATE LIMIT SCOPUS")
        print("=" * 60)
        print(f"📅 Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"🔑 API Key: {self.api_key[:8]}...{self.api_key[-4:]}")
        
        # Petición simple para verificar status
        test_url = f"{self.base_url}/content/search/scopus"
        test_params = {
            'query': 'TITLE("test")',
            'count': 1,
            'start': 0
        }
        
        try:
            print("\n🚀 Realizando petición de prueba...")
            response = requests.get(
                test_url, 
                headers=self.headers, 
                params=test_params,
                timeout=30
            )
            
            self.last_response = response
            self._extract_rate_limit_info(response)
            self._extract_error_info(response)
            self._print_detailed_report()
            
            return self.rate_limit_info
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Error de conexión: {e}")
            return None
    
    def _extract_rate_limit_info(self, response):
        """
        Extrae información de rate limit de los headers de respuesta
        """
        headers = response.headers
        
        # Headers típicos de rate limit de Elsevier
        self.rate_limit_info = {
            'status_code': response.status_code,
            'quota_exceeded': False,
            'rate_limit_exceeded': False,
            'requests_remaining': None,
            'quota_remaining': None,
            'reset_time': None,
            'all_headers': dict(headers)
        }
        
        # Buscar headers de rate limit (Elsevier usa varios formatos)
        rate_limit_headers = [
            'X-RateLimit-Remaining',
            'X-RateLimit-Limit', 
            'X-RateLimit-Reset',
            'X-ELS-ReqId',
            'X-ELS-APIKey-Remaining-Requests',
            'X-ELS-APIKey-Remaining-Requests-Per-Hour',
            'Retry-After'
        ]
        
        for header in rate_limit_headers:
            if header in headers:
                self.rate_limit_info[header.lower().replace('-', '_')] = headers[header]
        
        # Detectar si se excedió el rate limit
        if response.status_code == 429:
            self.rate_limit_info['rate_limit_exceeded'] = True
            
        # Detectar si se excedió la cuota
        if response.status_code in [403, 429]:
            error_text = response.text.lower()
            if any(word in error_text for word in ['quota', 'exceeded', 'limit']):
                self.rate_limit_info['quota_exceeded'] = True
    
    def _extract_error_info(self, response):
        """
        Extrae información detallada de errores
        """
        self.error_info = {
            'status_code': response.status_code,
            'status_text': response.reason,
            'has_error': response.status_code >= 400,
            'response_text': response.text[:500] if response.text else None,
            'content_type': response.headers.get('Content-Type', 'Unknown')
        }
        
        # Intentar parsear JSON de error
        try:
            if response.headers.get('Content-Type', '').startswith('application/json'):
                error_json = response.json()
                self.error_info['error_json'] = error_json
                
                # Buscar mensajes de error específicos
                if 'service-error' in error_json:
                    service_error = error_json['service-error']
                    self.error_info['error_code'] = service_error.get('status', {}).get('statusCode')
                    self.error_info['error_message'] = service_error.get('status', {}).get('statusText')
                    
        except json.JSONDecodeError:
            pass
    
    def _print_detailed_report(self):
        """
        Imprime un reporte detallado del estado
        """
        print("\n📊 REPORTE DE ESTADO:")
        print("-" * 40)
        
        # Estado general
        status = self.last_response.status_code
        if status == 200:
            print("✅ Status: OK (200)")
        elif status == 429:
            print("⚠️  Status: Rate Limit Exceeded (429)")
        elif status == 403:
            print("❌ Status: Forbidden (403) - Posible cuota excedida")
        else:
            print(f"⚠️  Status: {status} - {self.last_response.reason}")
        
        # Información de rate limit
        print("\n🔢 INFORMACIÓN DE RATE LIMIT:")
        for key, value in self.rate_limit_info.items():
            if key not in ['all_headers'] and value is not None:
                print(f"   {key}: {value}")
        
        # Errores específicos
        if self.error_info['has_error']:
            print("\n❌ INFORMACIÓN DE ERROR:")
            print(f"   Código: {self.error_info['status_code']}")
            print(f"   Mensaje: {self.error_info['status_text']}")
            
            if 'error_code' in self.error_info:
                print(f"   Error Code: {self.error_info['error_code']}")
            if 'error_message' in self.error_info:
                print(f"   Error Message: {self.error_info['error_message']}")
            
            # Mostrar respuesta completa de error
            if self.error_info['response_text']:
                print(f"\n📄 RESPUESTA COMPLETA DEL SERVIDOR:")
                print(f"   Content-Type: {self.error_info['content_type']}")
                print("   " + "-" * 50)
                print(f"   {self.error_info['response_text']}")
                print("   " + "-" * 50)
        
        # Headers importantes
        print("\n📋 HEADERS IMPORTANTES:")
        important_headers = [
            'X-ELS-ReqId', 'X-RateLimit-Remaining', 'Retry-After',
            'X-ELS-APIKey-Remaining-Requests', 'Date'
        ]
        
        for header in important_headers:
            if header in self.rate_limit_info['all_headers']:
                print(f"   {header}: {self.rate_limit_info['all_headers'][header]}")
        
        # TODOS los headers para análisis profundo
        print("\n🔍 TODOS LOS HEADERS (para análisis):")
        for header, value in self.rate_limit_info['all_headers'].items():
            print(f"   {header}: {value}")
    
    def test_multiple_requests(self, num_requests: int = 5):
        """
        Realiza múltiples peticiones para probar el comportamiento del rate limit
        """
        print(f"\n🧪 TESTING MÚLTIPLES PETICIONES ({num_requests})")
        print("=" * 60)
        
        results = []
        
        for i in range(num_requests):
            print(f"\n📡 Petición {i+1}/{num_requests}")
            
            test_url = f"{self.base_url}/content/search/scopus"
            test_params = {
                'query': f'TITLE("test{i}")',
                'count': 1,
                'start': 0
            }
            
            try:
                response = requests.get(
                    test_url,
                    headers=self.headers,
                    params=test_params,
                    timeout=10
                )
                
                result = {
                    'request_num': i+1,
                    'status_code': response.status_code,
                    'timestamp': datetime.now().strftime('%H:%M:%S'),
                    'rate_limit_exceeded': response.status_code == 429
                }
                
                # Extraer headers de rate limit
                if 'X-RateLimit-Remaining' in response.headers:
                    result['remaining'] = response.headers['X-RateLimit-Remaining']
                
                results.append(result)
                
                print(f"   Status: {response.status_code}")
                if response.status_code == 429:
                    print("   ⚠️  RATE LIMIT ALCANZADO")
                    break
                    
            except Exception as e:
                print(f"   ❌ Error: {e}")
                break
        
        # Resumen de resultados
        print(f"\n📈 RESUMEN DE {len(results)} PETICIONES:")
        for result in results:
            status_icon = "✅" if result['status_code'] == 200 else "❌"
            print(f"   {status_icon} #{result['request_num']}: {result['status_code']} a las {result['timestamp']}")
        
        return results
    
    def get_api_quota_info(self):
        """
        Obtiene información específica de cuota de la API
        """
        print("\n📊 INFORMACIÓN DE CUOTA API:")
        print("-" * 40)
        
        # Información conocida de Scopus
        print("📋 LÍMITES CONOCIDOS DE SCOPUS:")
        print("   • Cuota semanal: 20,000 peticiones")
        print("   • Rate limit: 9-15 peticiones/segundo")
        print("   • Reseteo: Semanal (lunes)")
        
        if self.rate_limit_info:
            print(f"\n📈 ESTADO ACTUAL:")
            if self.rate_limit_info.get('quota_exceeded'):
                print("   ❌ CUOTA SEMANAL EXCEDIDA")
            elif self.rate_limit_info.get('rate_limit_exceeded'):
                print("   ⚠️  RATE LIMIT TEMPORAL EXCEDIDO")
            else:
                print("   ✅ API DISPONIBLE")


def main():
    """
    Función principal para ejecutar el monitor
    """
    monitor = ScopusRateLimitMonitor()
    
    # Verificar estado actual
    monitor.check_rate_limit_status()
    
    # Información de cuota
    monitor.get_api_quota_info()
    
    # Test opcional de múltiples peticiones
    response = input("\n¿Quieres probar múltiples peticiones? (y/n): ")
    if response.lower() == 'y':
        num_requests = int(input("¿Cuántas peticiones? (recomendado: 3-5): "))
        monitor.test_multiple_requests(num_requests)
    
    print("\n🏁 MONITOR COMPLETADO")


if __name__ == "__main__":
    main()
