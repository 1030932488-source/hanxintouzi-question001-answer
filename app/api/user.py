from flask import Blueprint, render_template, redirect, url_for, session
from functools import wraps
from app.models import db, User, LoginLog

user_bp = Blueprint('user', __name__, url_prefix='/user') # Added url_prefix for better structure

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('auth.login'))
        return f(*args, **kwargs)
    return decorated_function

@user_bp.route('/')
@login_required
def index():
    user = User.query.get(session['user_id'])
    if not user:
        session.clear()
        return redirect(url_for('auth.login'))
    return render_template('dashboard.html', user=user)

@user_bp.route('/profile')
@login_required
def profile():
    user = User.query.get(session['user_id'])
    if not user:
        session.clear()
        return redirect(url_for('auth.login'))
    
    login_logs = LoginLog.query.filter_by(user_id=user.id)\
        .order_by(LoginLog.login_time.desc())\
        .limit(10)\
        .all()
    
    return render_template('profile.html', user=user, login_logs=login_logs)
