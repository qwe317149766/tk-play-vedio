// 服务类型配置
const baseServices = {
	playVedio: { name: 'TikTok播放', icon: '▶️', unit: '1000次播放' },
	likeVedio: { name: 'TikTok点赞', icon: '❤️', unit: '1000个' },
	commentVedio: { name: 'TikTok评论', icon: '💬', unit: '1000条' },
	followVedio: { name: 'TikTok私信', icon: '💌', unit: '1000条' },
}

export const SERVICE_TYPES = {
	playVedio: { ...baseServices.playVedio, key: 'playVedio' },
	likeVedio: { ...baseServices.likeVedio, key: 'likeVedio' },
	commentVedio: { ...baseServices.commentVedio, key: 'commentVedio' },
	followVedio: { ...baseServices.followVedio, key: 'followVedio' },
	play: { ...baseServices.playVedio, key: 'play' },
	like: { ...baseServices.likeVedio, key: 'like' },
	comment: { ...baseServices.commentVedio, key: 'comment' },
	follow: { ...baseServices.followVedio, key: 'follow' },
}

export const SERVICE_KEY_MAP = {
	playVedio: ['playVedio', 'play'],
	likeVedio: ['likeVedio', 'like'],
	commentVedio: ['commentVedio', 'comment'],
	followVedio: ['followVedio', 'follow'],
}

// 订单状态映射
export const ORDER_STATUS = {
	0: { text: '待处理', class: 'pending' },
	1: { text: '处理中', class: 'processing' },
	2: { text: '已完成', class: 'completed' },
}
