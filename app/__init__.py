from flask import Flask, redirect, url_for
from app.extensions import db
from config import Config

def create_app(config_class=Config):
    app = Flask(__name__)
    app.config.from_object(config_class)
    
    # Initialize Flask extensions
    db.init_app(app)
    
    # Register Blueprints
    from app.api.auth import auth_bp
    from app.api.user import user_bp
    
    app.register_blueprint(auth_bp)
    app.register_blueprint(user_bp)
    
    @app.route('/')
    def root():
        return redirect(url_for('auth.index'))
        
    with app.app_context():
        db.create_all()
        
    return app
