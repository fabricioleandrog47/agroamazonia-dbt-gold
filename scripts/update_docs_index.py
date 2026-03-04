#!/usr/bin/env python3
"""
Script para atualizar o índice de projetos DBT no S3
"""

import boto3
import json
import os
from datetime import datetime

def update_docs_index():
    """Atualiza o índice de projetos no S3"""
    
    s3 = boto3.client('s3')
    bucket = 'brid-dbt-docs'
    
    try:
        # Listar projetos no bucket
        response = s3.list_objects_v2(Bucket=bucket, Delimiter='/')
        
        projects = []
        for prefix in response.get('CommonPrefixes', []):
            project_name = prefix['Prefix'].rstrip('/')
            
            # Verificar se tem index.html (documentação DBT)
            try:
                s3.head_object(Bucket=bucket, Key=f'{project_name}/index.html')
                projects.append({
                    'name': project_name,
                    'url': f'/{project_name}/',
                    'updated': datetime.now().strftime('%d/%m/%Y %H:%M')
                })
            except:
                # Projeto sem documentação ainda
                continue
        
        # Gerar projects.json
        projects_json = json.dumps(projects, indent=2)
        
        # Upload projects.json
        s3.put_object(
            Bucket=bucket,
            Key='projects.json',
            Body=projects_json,
            ContentType='application/json'
        )
        
        print(f"✅ Índice atualizado com {len(projects)} projetos")
        
        # Gerar index.html melhorado
        html_content = generate_index_html(projects)
        
        # Upload index.html
        s3.put_object(
            Bucket=bucket,
            Key='index.html',
            Body=html_content,
            ContentType='text/html'
        )
        
        print("✅ Página principal atualizada")
        
    except Exception as e:
        print(f"❌ Erro ao atualizar índice: {e}")

def generate_index_html(projects):
    """Gera HTML da página principal"""
    
    projects_html = ""
    for project in projects:
        projects_html += f"""
        <div class="project">
            <h3><a href="{project['url']}">{project['name']}</a></h3>
            <p>Documentação DBT do projeto {project['name']}</p>
            <small>Atualizado em: {project['updated']}</small>
        </div>
        """
    
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>DBT Documentation Hub</title>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <style>
            body {{ 
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                margin: 0;
                padding: 40px;
                background-color: #f8f9fa;
            }}
            .container {{
                max-width: 1200px;
                margin: 0 auto;
                background: white;
                padding: 40px;
                border-radius: 8px;
                box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            }}
            h1 {{ 
                color: #333;
                margin-bottom: 10px;
            }}
            .subtitle {{
                color: #666;
                margin-bottom: 40px;
                font-size: 18px;
            }}
            .project {{ 
                margin: 20px 0;
                padding: 20px;
                border: 1px solid #e9ecef;
                border-radius: 8px;
                background: #fff;
                transition: box-shadow 0.2s;
            }}
            .project:hover {{
                box-shadow: 0 4px 12px rgba(0,0,0,0.1);
            }}
            .project h3 {{
                margin: 0 0 10px 0;
            }}
            .project a {{ 
                text-decoration: none;
                color: #0066cc;
                font-weight: 600;
            }}
            .project a:hover {{ 
                text-decoration: underline;
            }}
            .project p {{
                margin: 10px 0;
                color: #666;
            }}
            .project small {{
                color: #999;
            }}
            .stats {{
                background: #e3f2fd;
                padding: 20px;
                border-radius: 8px;
                margin-bottom: 30px;
                text-align: center;
            }}
            .search {{
                margin-bottom: 30px;
            }}
            .search input {{
                width: 100%;
                padding: 12px;
                border: 1px solid #ddd;
                border-radius: 6px;
                font-size: 16px;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>🚀 DBT Documentation Hub</h1>
            <p class="subtitle">Documentação centralizada de todos os projetos DBT</p>
            
            <div class="stats">
                <strong>{len(projects)} projetos documentados</strong>
            </div>
            
            <div class="search">
                <input type="text" id="searchInput" placeholder="Buscar projetos..." onkeyup="filterProjects()">
            </div>
            
            <div id="projects">
                {projects_html}
            </div>
        </div>
        
        <script>
            function filterProjects() {{
                const input = document.getElementById('searchInput');
                const filter = input.value.toLowerCase();
                const projects = document.querySelectorAll('.project');
                
                projects.forEach(project => {{
                    const text = project.textContent.toLowerCase();
                    if (text.includes(filter)) {{
                        project.style.display = '';
                    }} else {{
                        project.style.display = 'none';
                    }}
                }});
            }}
        </script>
    </body>
    </html>
    """

if __name__ == "__main__":
    update_docs_index()