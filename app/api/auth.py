from flask import Blueprint, render_template, request, redirect, url_for, flash, session
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime
from app.models import db, User, LoginLog

auth_bp = Blueprint('auth', __name__)

@auth_bp.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        email = request.form.get('email', '').strip()
        password = request.form.get('password', '')
        
        if not username or not email or not password:
            flash('请填写所有字段')
            return render_template('register.html')
        
        if User.query.filter_by(username=username).first():
            flash('用户名已存在')
            return render_template('register.html')
        
        if User.query.filter_by(email=email).first():
            flash('邮箱已存在')
            return render_template('register.html')
        
        try:
            user = User(
                username=username,
                email=email,
                password_hash=generate_password_hash(password)
            )
            db.session.add(user)
            db.session.commit()
            
            flash('注册成功，请登录')
            return redirect(url_for('auth.login'))
        except Exception as e:
            db.session.rollback()
            flash(f'注册失败：{str(e)}')
            return render_template('register.html')
    
    return render_template('register.html')

@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '')
        
        if not username or not password:
            flash('请输入用户名和密码')
            return render_template('login.html')
        
        user = User.query.filter_by(username=username).first()
        
        if user and check_password_hash(user.password_hash, password):
            session['user_id'] = user.id
            session['username'] = user.username
            session.permanent = True
            
            user.last_login = datetime.utcnow()
            
            try:
                login_log = LoginLog(
                    user_id=user.id,
                    ip_address=request.remote_addr
                )
                db.session.add(login_log)
                db.session.commit()
                session['login_log_id'] = login_log.id
            except Exception as e:
                db.session.rollback()
                print(f'登录日志记录失败：{str(e)}')
            
            flash('登录成功')
            return redirect(url_for('user.index'))
        else:
            flash('用户名或密码错误')
    
    return render_template('login.html')

@auth_bp.route('/logout')
def logout():
    if 'user_id' in session and 'login_log_id' in session:
        try:
            login_log = LoginLog.query.get(session['login_log_id'])
            if login_log:
                login_log.logout_time = datetime.utcnow()
                db.session.commit()
        except Exception as e:
            db.session.rollback()
            print(f'登出日志更新失败：{str(e)}')
    
    session.clear()
    flash('已登出')
    return redirect(url_for('auth.login'))

@auth_bp.route('/')
def index():
    if 'user_id' in session:
        return redirect(url_for('user.index'))
    return redirect(url_for('auth.login'))
