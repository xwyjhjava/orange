CREATE TABLE `dreams`.`tb_video` (
  `id` INT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `type` INT UNSIGNED NULL COMMENT '类别',
  `title` VARCHAR(45) NULL COMMENT '视频标题',
  `uploadtime` DATETIME NULL COMMENT '上传时间',
  `url` VARCHAR(45) NULL COMMENT 'url',
  `author` VARCHAR(45) NULL COMMENT '作者',
  `likenum` INT UNSIGNED NULL COMMENT '点赞数量',
  `coinnum` INT UNSIGNED NULL COMMENT '投币数量',
  `collectionnum` INT UNSIGNED NULL COMMENT '收藏数量',
  `brief` VARCHAR(45) NULL COMMENT '视频简介',
  `tag` VARCHAR(45) NULL COMMENT '标签',
  `comment` VARCHAR(255) NULL COMMENT '评论',
  PRIMARY KEY (`id`))
COMMENT = '视频表';
