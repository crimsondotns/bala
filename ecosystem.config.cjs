module.exports = {
  apps: [
    {
      name: 'solana-token-tracker',
      script: './index.js',
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: '500M',
      env: {
        NODE_ENV: 'development',
      },
      env_production: {
        NODE_ENV: 'production',
      },
      time: true, // adds timestamp to pm2 logs
    },
  ],
};
