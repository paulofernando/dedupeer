package deduplication.gui.component.renderer;

import java.awt.Color;
import java.awt.Component;
import java.awt.GradientPaint;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.geom.Rectangle2D;

import javax.swing.JProgressBar;
import javax.swing.JTable;
import javax.swing.table.TableCellRenderer;

public class JProgressRenderer extends JProgressBar implements TableCellRenderer {

	private static final long serialVersionUID = -4116117091520144073L;
	private Color background;
	private ProgressInfo progressInfo;
	
	Color[][] colors = new Color[][]{
			{new Color(0xa4dcb2), new Color(0x61e868)},
			{new Color(0xc0cde0), new Color(0x76a1e3)},
			{new Color(0xe8e6dd), new Color(0xe8c53a)}			
	};
	
	@Override
	public Component getTableCellRendererComponent(JTable table, Object value,
			boolean selected, boolean hasFocus, int row, int column) {
		
		progressInfo = (ProgressInfo) value;
		this.setValue(progressInfo.getProgress());
		
		if(selected) {
			background = table.getSelectionBackground();
		} else {
			background = table.getBackground();
		}
		invalidate();
		return this;
	}
	
	public void paint(Graphics graphics) {
		super.paint(graphics);
		if(this.getValue() == 0) {
			graphics.setColor(background);
			graphics.fillRect(0, 0, this.getWidth(), this.getHeight());
		} else {
			Graphics2D g2 = (Graphics2D) graphics;
	        Rectangle2D r = new Rectangle2D.Double(2, 1, (progressInfo.getProgress() * this.getWidth()) / 100, this.getHeight() - 2);
	        GradientPaint gp = new GradientPaint(0, 0, colors[progressInfo.getType()][0], 0, this.getHeight(),
	        		colors[progressInfo.getType()][1], true);
	        g2.setPaint(gp);
	        g2.fill(r);
			
			graphics.setColor(new Color(0x333333));
			graphics.drawString(progressInfo.getTypeString(), 2, ((this.getHeight()>>1) + (graphics.getFontMetrics().getHeight()>>1)));
		}
	}
}
