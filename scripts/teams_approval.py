#!/usr/bin/env python3
"""
Script para integração com Microsoft Teams para aprovação de deploys
"""

import os
import json
import requests
import time
from datetime import datetime
from typing import Dict, Any

class TeamsDeployApproval:
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url
        self.approval_timeout = 3600  # 1 hora
        
    def send_approval_request(self, deploy_info: Dict[str, Any]) -> bool:
        """Envia solicitação de aprovação para o Teams"""
        
        payload = {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "themeColor": "0076D7",
            "summary": "Aprovação necessária para deploy em produção",
            "sections": [{
                "activityTitle": "🚀 Deploy para Produção - Aprovação Necessária",
                "activitySubtitle": f"Pipeline: {deploy_info.get('pipeline_name', 'dbt-eks-template')} | ⚠️ Clique 'Aprovar Deploy' → Actions → Workflow atual → Link da Issue",
                "activityImage": "https://github.com/fluidicon.png",
                "facts": [
                    {"name": "📁 Repositório:", "value": deploy_info.get('repository', 'N/A')},
                    {"name": "🌿 Branch:", "value": deploy_info.get('branch', 'N/A')},
                    {"name": "🔗 Commit SHA:", "value": f"`{deploy_info.get('commit_sha', 'N/A')}`"},
                    {"name": "👤 Autor:", "value": deploy_info.get('author', 'N/A')},
                    {"name": "💬 Mensagem:", "value": f"_{deploy_info.get('commit_message', 'N/A')}_"},
                    {"name": "📊 Arquivos Alterados:", "value": deploy_info.get('files_changed', 'N/A')},
                    {"name": "🔴 Ambiente:", "value": "**PRODUÇÃO**"},
                    {"name": "⏰ Horário:", "value": datetime.now().strftime("%d/%m/%Y %H:%M:%S")}
                ],
                "markdown": True
            }],
            "potentialAction": [
                {
                    "@type": "OpenUri",
                    "name": "✅ Aprovar Deploy",
                    "targets": [{
                        "os": "default",
                        "uri": f"https://github.com/{deploy_info.get('repository', '')}/actions"
                    }]
                },
                {
                    "@type": "OpenUri",
                    "name": "📝 Ver Commit",
                    "targets": [{
                        "os": "default",
                        "uri": f"https://github.com/{deploy_info.get('repository', '')}/commit/{deploy_info.get('full_commit_sha', '')}"
                    }]
                },
                {
                    "@type": "OpenUri",
                    "name": "📋 Ver Pipeline",
                    "targets": [{
                        "os": "default",
                        "uri": deploy_info.get('details_url', '#')
                    }]
                }
            ]
        }
        
        try:
            response = requests.post(self.webhook_url, json=payload)
            response.raise_for_status()
            print("✅ Notificação enviada para o Teams com sucesso")
            return True
        except requests.exceptions.RequestException as e:
            print(f"❌ Erro ao enviar notificação: {e}")
            return False
    
    def send_issue_notification(self, deploy_info: Dict[str, Any]):
        """Envia notificação com link direto para a Issue de aprovação"""
        
        payload = {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "themeColor": "0076D7",
            "summary": "Aprovação necessária para deploy em produção",
            "sections": [{
                "activityTitle": "🚀 Deploy para Produção - Aprovação Necessária",
                "activitySubtitle": f"Pipeline: {deploy_info.get('pipeline_name', 'dbt-eks-template')}",
                "activityImage": "https://github.com/fluidicon.png",
                "facts": [
                    {"name": "📁 Repositório:", "value": deploy_info.get('repository', 'N/A')},
                    {"name": "🌿 Branch:", "value": deploy_info.get('branch', 'N/A')},
                    {"name": "🔗 Commit SHA:", "value": f"`{deploy_info.get('commit_sha', 'N/A')}`"},
                    {"name": "👤 Autor:", "value": deploy_info.get('author', 'N/A')},
                    {"name": "💬 Mensagem:", "value": f"_{deploy_info.get('commit_message', 'N/A')}_"},
                    {"name": "📊 Arquivos Alterados:", "value": deploy_info.get('files_changed', 'N/A')},
                    {"name": "🔴 Ambiente:", "value": "**PRODUÇÃO**"},
                    {"name": "⏰ Horário:", "value": datetime.now().strftime("%d/%m/%Y %H:%M:%S")}
                ],
                "markdown": True
            }],
            "potentialAction": [
                {
                    "@type": "OpenUri",
                    "name": "✅ Aprovar Deploy",
                    "targets": [{
                        "os": "default",
                        "uri": deploy_info.get('issue_url', '#')
                    }]
                },
                {
                    "@type": "OpenUri",
                    "name": "📝 Ver Commit",
                    "targets": [{
                        "os": "default",
                        "uri": f"https://github.com/{deploy_info.get('repository', '')}/commit/{deploy_info.get('full_commit_sha', '')}"
                    }]
                },
                {
                    "@type": "OpenUri",
                    "name": "📋 Ver Pipeline",
                    "targets": [{
                        "os": "default",
                        "uri": deploy_info.get('details_url', '#')
                    }]
                }
            ]
        }
        
        try:
            response = requests.post(self.webhook_url, json=payload)
            response.raise_for_status()
            print("✅ Notificação de aprovação enviada para o Teams com sucesso")
        except requests.exceptions.RequestException as e:
            print(f"❌ Erro ao enviar notificação de aprovação: {e}")

    def send_status_notification(self, status: str, deploy_info: Dict[str, Any]):
        """Envia notificação de status do deploy"""
        
        if status == "success":
            color = "00FF00"
            title = "✅ Deploy Concluído com Sucesso"
            emoji = "🎉"
        elif status == "failure":
            color = "FF0000" 
            title = "❌ Falha no Deploy"
            emoji = "💥"
        elif status == "cancelled":
            color = "FFA500"
            title = "⚠️ Deploy Cancelado"
            emoji = "🛑"
        else:
            color = "808080"
            title = "ℹ️ Status do Deploy"
            emoji = "📊"
            
        payload = {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions", 
            "themeColor": color,
            "summary": f"Deploy {status}",
            "sections": [{
                "activityTitle": f"{emoji} {title}",
                "activitySubtitle": f"Pipeline: {deploy_info.get('pipeline_name', 'dbt-eks-template')}",
                "facts": [
                    {"name": "Repositório:", "value": deploy_info.get('repository', 'N/A')},
                    {"name": "Branch:", "value": deploy_info.get('branch', 'N/A')},
                    {"name": "Commit:", "value": deploy_info.get('commit_sha', 'N/A')},
                    {"name": "Duração:", "value": deploy_info.get('duration', 'N/A')},
                    {"name": "Horário:", "value": datetime.now().strftime("%d/%m/%Y %H:%M:%S")}
                ]
            }]
        }
        
        if status == "failure":
            payload["potentialAction"] = [{
                "@type": "OpenUri",
                "name": "Ver Logs de Erro",
                "targets": [{
                    "os": "default",
                    "uri": deploy_info.get('logs_url', '#')
                }]
            }]
        
        try:
            response = requests.post(self.webhook_url, json=payload)
            response.raise_for_status()
            print(f"✅ Notificação de {status} enviada com sucesso")
        except requests.exceptions.RequestException as e:
            print(f"❌ Erro ao enviar notificação de status: {e}")

def main():
    """Função principal para uso em CI/CD"""
    
    # Configurações do ambiente
    webhook_url = os.getenv('TEAMS_WEBHOOK_URL')
    if not webhook_url:
        print("❌ TEAMS_WEBHOOK_URL não configurado")
        exit(1)
    
    # Informações do deploy (vindas do CI/CD)
    deploy_info = {
        'pipeline_name': os.getenv('PIPELINE_NAME', 'dbt-eks-template'),
        'repository': os.getenv('GITHUB_REPOSITORY', 'N/A'),
        'branch': os.getenv('GITHUB_REF_NAME', 'N/A'),
        'commit_sha': os.getenv('GITHUB_SHA', 'N/A')[:8],
        'full_commit_sha': os.getenv('GITHUB_SHA', 'N/A'),
        'author': os.getenv('GITHUB_ACTOR', 'N/A'),
        'commit_message': os.getenv('COMMIT_MESSAGE', 'N/A'),
        'files_changed': os.getenv('FILES_CHANGED', 'Ver no commit'),
        'deploy_id': os.getenv('GITHUB_RUN_ID', 'N/A'),
        'details_url': f"https://github.com/{os.getenv('GITHUB_REPOSITORY', '')}/actions/runs/{os.getenv('GITHUB_RUN_ID', '')}",
        'logs_url': f"https://github.com/{os.getenv('GITHUB_REPOSITORY', '')}/actions/runs/{os.getenv('GITHUB_RUN_ID', '')}"
    }
    
    teams = TeamsDeployApproval(webhook_url)
    
    # Enviar solicitação de aprovação
    action = os.getenv('ACTION', 'request_approval')
    
    if action == 'request_approval':
        teams.send_approval_request(deploy_info)
    elif action == 'issue_created':
        # Enviar notificação com link direto para a Issue
        issue_number = os.getenv('ISSUE_NUMBER', 'N/A')
        deploy_info['issue_url'] = f"https://github.com/{deploy_info['repository']}/issues/{issue_number}"
        teams.send_issue_notification(deploy_info)
    elif action in ['success', 'failure', 'cancelled']:
        deploy_info['duration'] = os.getenv('DEPLOY_DURATION', 'N/A')
        teams.send_status_notification(action, deploy_info)

if __name__ == "__main__":
    main()